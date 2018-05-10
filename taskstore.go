// Copyright 2014 Chris Monson <shiblon@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package taskstore implements a library for a simple task store.
// This provides abstractions for creating a simple task store process that
// manages data in memory and on disk. It can be used to implement a full-fledged
// task queue, but it is only the core storage piece. It does not, in particular,
// implement any networking.
package taskstore // import "entrogo.com/taskstore"

import (
	"errors"
	"fmt"
	"strings"

	"time"

	"entrogo.com/taskstore/journal"
	"entrogo.com/taskstore/keyheap"
)

var (
	ErrAlreadySnapshotting = errors.New("already snapshotting")
	ErrJournalClosed       = errors.New("journal closed")
	ErrAlreadyClosed       = errors.New("already closed")
)

// TaskStore maintains the tasks.
type TaskStore struct {
	// A heap for each group.
	heaps map[string]*keyheap.KeyHeap

	// All tasks known to this TaskStore.
	tasks map[int64]*Task

	lastTaskID int64

	// When the tasks are being snapshotted, these are used to keep throughput
	// going while the tasks map is put into read-only mode.
	tmpTasks map[int64]*Task
	delTasks map[int64]bool

	// The journal utility that actually does the work of appending and
	// rotating.
	journaler journal.Interface

	// To write to the journal opportunistically, push transactions into this
	// channel.
	journalChan chan []updateDiff

	snapshotting      bool
	txnsSinceSnapshot int

	// Channels for making various requests to the task store.
	updateChan       chan request
	listGroupChan    chan request
	claimChan        chan request
	groupsChan       chan request
	snapshottingChan chan request
	snapshotChan     chan request
	snapshotDoneChan chan error
	stringChan       chan request
	allTasksChan     chan request
	numTasksChan     chan request
	latestTaskIDChan chan request
	tasksChan        chan request
	closeChan        chan request
	isOpenChan       chan request
}

// OpenStrict returns a TaskStore with journaling done synchronously
// instead of opportunistically. This means that, in the event of a crash, the
// full task state will be recoverable and nothing will be lost that appeared
// to be commmitted.
// Use this if you don't mind slower mutations and really need committed tasks
// to stay committed under all circumstances. In particular, if task execution
// is not idempotent, this is the right one to use.
func OpenStrict(journaler journal.Interface) (*TaskStore, error) {
	return openTaskStoreHelper(journaler, false)
}

// OpenOpportunistic returns a new TaskStore instance.
// This store will be opportunistically journaled, meaning that it is possible
// to update, delete, or create a task, get confirmation of it occurring,
// crash, and find that recently committed tasks are lost.
// If task execution is idempotent, this is safe, and is much faster, as it
// writes to disk when it gets a chance.
func OpenOpportunistic(journaler journal.Interface) (*TaskStore, error) {
	return openTaskStoreHelper(journaler, true)
}

func (t *TaskStore) IsStrict() bool {
	return t.journalChan == nil
}

func (t *TaskStore) IsOpen() bool {
	resp := t.sendRequest(nil, t.isOpenChan)
	return resp.Val.(bool)
}

// Close shuts down the taskstore gracefully, finalizing the journal, etc.
func (t *TaskStore) Close() error {
	resp := t.sendRequest(nil, t.closeChan)
	return resp.Err
}

// String formats this as a string. Shows minimal information like group names.
func (t *TaskStore) String() string {
	resp := t.sendRequest(nil, t.stringChan)
	return resp.Val.(string)
}

// UpdateError contains a map of errors, the key is the index of a task that
// was not present in an expected way. All fields are nil when empty.
type UpdateError struct {
	// Changes contains the list of tasks that were not present and could thus not be changed.
	Changes []int64

	// Deletes contains the list of IDs that could not be deleted.
	Deletes []int64

	// Depends contains the list of IDs that were not present and caused the update to fail.
	Depends []int64

	// Owned contains the list of IDs that were owned by another client and could not be changed.
	Owned []int64

	// Bugs contains a list of errors representing caller precondition failures (bad inputs).
	Bugs []error
}

func (ue UpdateError) HasDependencyErrors() bool {
	return len(ue.Changes) > 0 || len(ue.Deletes) > 0 || len(ue.Depends) > 0 || len(ue.Owned) > 0
}

func (ue UpdateError) HasBugs() bool {
	return len(ue.Bugs) > 0
}

func (ue UpdateError) HasErrors() bool {
	return ue.HasDependencyErrors() || ue.HasBugs()
}

// Error returns an error string (and satisfies the Error interface).
func (ue UpdateError) Error() string {
	strs := []string{"update error:"}
	if len(ue.Changes) > 0 {
		strs = append(strs, fmt.Sprintf("  Change IDs: %d", ue.Changes))
	}
	if len(ue.Deletes) > 0 {
		strs = append(strs, fmt.Sprintf("  Delete IDs: %d", ue.Deletes))
	}
	if len(ue.Depends) > 0 {
		strs = append(strs, fmt.Sprintf("  Depend IDs: %d", ue.Depends))
	}
	if len(ue.Owned) > 0 {
		strs = append(strs, fmt.Sprintf("  Owned IDs: %d", ue.Owned))
	}
	if len(ue.Bugs) > 0 {
		strs = append(strs, "  Bugs:")
		for _, e := range ue.Bugs {
			strs = append(strs, fmt.Sprintf("    %v", e))
		}
	}
	return strings.Join(strs, "\n")
}

// Update makes changes to the task store. The owner is the ID of the
// requester, and tasks to be added, changed, and deleted can be specified. If
// dep is specified, it is a list of task IDs that must be present for the
// update to succeed.
// On success, the returned slice of tasks will contain the concatenation of
// newly added tasks and changed tasks, in order
// (e.g., [add0, add1, add2, change0, change1, change2]).
// On failure, an error of type UpdateError will be returned with details about
// the types of errors and the IDs that caused them.
func (t *TaskStore) Update(owner int32, add, change []*Task, del, dep []int64) ([]*Task, error) {
	up := reqUpdate{
		OwnerID: owner,
		Changes: make([]*Task, 0, len(add)+len(change)),
		Deletes: del,
		Depends: dep,
	}

	for _, task := range add {
		task := task.Copy()
		task.ID = 0          // ensure that it's really an add.
		task.OwnerID = owner // require that the owner be the requester.
		if task.AT < 0 {
			task.AT = 0 // ensure that it doesn't get marked for deletion.
		}
		up.Changes = append(up.Changes, task)
	}

	for _, task := range change {
		task := task.Copy()
		task.OwnerID = owner
		if task.AT < 0 {
			task.AT = 0 // no accidental deletions
		}
		up.Changes = append(up.Changes, task)
	}

	resp := t.sendRequest(up, t.updateChan)
	if resp.Err != nil {
		return nil, resp.Err
	}
	return resp.Val.([]*Task), resp.Err
}

// AllTasks returns a slice of every task in the store, sorted by ID. This can
// be an expensive operation, as it blocks all access while it copies the
// list of tasks, so don't do it at all when you care deeply about availability.
func (t *TaskStore) AllTasks() []*Task {
	resp := t.sendRequest(nil, t.allTasksChan)
	return resp.Val.([]*Task)
}

// ListGroup tries to find tasks for the given group name. The number of tasks
// returned will be no more than the specified limit. A limit of 0 or less
// indicates that all possible tasks should be returned. If allowOwned is
// specified, then even tasks with AT in the future that are owned
// by other clients will be returned.
func (t *TaskStore) ListGroup(name string, limit int, allowOwned bool) []*Task {
	lg := reqListGroup{
		Name:       name,
		Limit:      limit,
		AllowOwned: allowOwned,
	}
	resp := t.sendRequest(lg, t.listGroupChan)
	return resp.Val.([]*Task)
}

// Groups returns a list of all of the groups known to this task store.
func (t *TaskStore) Groups() []string {
	resp := t.sendRequest(nil, t.groupsChan)
	return resp.Val.([]string)
}

// NumTasks returns the number of tasks being managed by this store.
func (t *TaskStore) NumTasks() int {
	resp := t.sendRequest(nil, t.numTasksChan)
	return int(resp.Val.(int64))
}

// LatestTaskID returns the most recently-assigned task ID.
func (t *TaskStore) LatestTaskID() int64 {
	resp := t.sendRequest(nil, t.latestTaskIDChan)
	return resp.Val.(int64)
}

// Claim attempts to find one random unowned task in the specified group and
// set the ownership to the specified owner. If successful, the newly-owned
// tasks are returned with their AT set to now + duration (in
// nanoseconds).
func (t *TaskStore) Claim(owner int32, group string, duration int64, depends []int64) (*Task, error) {
	claim := reqClaim{
		OwnerID:  owner,
		Group:    group,
		Duration: duration,
		Depends:  depends,
	}
	resp := t.sendRequest(claim, t.claimChan)
	if resp.Err != nil {
		return nil, resp.Err
	}
	return resp.Val.(*Task), nil
}

// Tasks attempts to retrieve particular tasks from the store, specified by ID.
// The returned slice of tasks will be of the same size as the requested IDs,
// and some of them may be nil (if the requested task does not exist).
func (t *TaskStore) Tasks(ids []int64) []*Task {
	resp := t.sendRequest(ids, t.tasksChan)
	return resp.Val.([]*Task)
}

// Snapshotting indicates whether snapshotting is in progress.
func (t *TaskStore) Snapshotting() bool {
	resp := t.sendRequest(nil, t.snapshottingChan)
	return resp.Val.(bool)
}

// Snapshot tries to force a snapshot to start immediately. It only fails if
// there is already one in progress.
func (t *TaskStore) Snapshot() error {
	resp := t.sendRequest(nil, t.snapshotChan)
	return resp.Err
}

// Now returns the current time in nanoseconds since the UTC epoch. This is the
// standard Go time granularity, so it works in all functions needing time
// without being multiplied by a time.Duration constant.
func Now() int64 {
	return time.Now().UnixNano()
}
