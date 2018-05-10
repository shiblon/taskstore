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

package taskstore // import "entrogo.com/taskstore"

import (
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"time"

	"entrogo.com/taskstore/journal"
	"entrogo.com/taskstore/keyheap"
)

// TODO: move snapshot functionality completely out, make it operate only on files.
// This might require making a way to open a read-only taskstore, then a way to
// dump it to a file.

const (
	// The maximum number of items to deplete from the cache when snapshotting
	// is finished but the cache has items in it (during an update).
	maxCacheDepletion = 20

	// The maximum number of transactions to journal before snapshotting.
	maxTxnsSinceSnapshot = 30000
)

// getTask returns the task with the given ID if it exists, else nil.
func (t *TaskStore) getTask(id int64) *Task {
	if id <= 0 {
		// Invalid ID.
		return nil
	}
	if _, ok := t.delTasks[id]; ok {
		// Already deleted in the temporary cache.
		return nil
	}
	if t, ok := t.tmpTasks[id]; ok {
		// Sitting in cache.
		return t
	}
	if t, ok := t.tasks[id]; ok {
		// Sitting in the main index.
		return t
	}
	return nil
}

func openTaskStoreHelper(journaler journal.Interface, opportunistic bool) (*TaskStore, error) {
	if journaler == nil || !journaler.IsOpen() {
		return nil, ErrJournalClosed
	}

	var err error

	// Before starting our handler and allowing requests to come in, try
	// reading the latest snapshot and journals.
	sdec, err := journaler.SnapshotDecoder()
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("snapshot error: %v", err)
	}
	jdec, err := journaler.JournalDecoder()
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("journar error: %v", err)
	}

	ts := &TaskStore{
		journaler: journaler,

		heaps:    make(map[string]*keyheap.KeyHeap),
		tasks:    make(map[int64]*Task),
		tmpTasks: make(map[int64]*Task),
		delTasks: make(map[int64]bool),

		updateChan:       make(chan request),
		listGroupChan:    make(chan request),
		claimChan:        make(chan request),
		groupsChan:       make(chan request),
		snapshottingChan: make(chan request),
		snapshotChan:     make(chan request),
		snapshotDoneChan: make(chan error),
		stringChan:       make(chan request),
		allTasksChan:     make(chan request),
		numTasksChan:     make(chan request),
		latestTaskIDChan: make(chan request),
		tasksChan:        make(chan request),
		closeChan:        make(chan request),
		isOpenChan:       make(chan request),
	}

	// Read the snapshot.
	task := new(Task)
	err = sdec.Decode(task)
	for err != io.EOF {
		if _, ok := ts.tasks[task.ID]; ok {
			return nil, fmt.Errorf("can't happen - two tasks with same ID %d in snapshot", task.ID)
		}
		ts.tasks[task.ID] = task
		if ts.lastTaskID < task.ID {
			ts.lastTaskID = task.ID
		}
		if _, ok := ts.heaps[task.Group]; !ok {
			ts.heaps[task.Group] = keyheap.New()
		}
		ts.heaps[task.Group].Push(task)

		// Get ready for a new round.
		task = new(Task)
		err = sdec.Decode(task)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("corrupt snapshot: %v", err)
		}
	}

	// Replay the journals.
	transaction := new([]updateDiff)
	err = jdec.Decode(transaction)
	for err != io.EOF && err != io.ErrUnexpectedEOF {
		// Replay this transaction - not busy snapshotting.
		ts.playTransaction(*transaction, false)
		ts.txnsSinceSnapshot++

		// Next record
		transaction = new([]updateDiff)
		err = jdec.Decode(transaction)
		if err == io.ErrUnexpectedEOF {
			log.Println("Found unexpected EOF in journal stream. Continuing.")
		} else if err != nil && err != io.EOF {
			return nil, fmt.Errorf("corrupt journal: %v", err)
		}
	}

	if opportunistic {
		// non-nil journalChan means "append opportunistically" and frees up
		// the journalChan case in "handle".
		ts.journalChan = make(chan []updateDiff, 1)
	}

	// Everything is ready, now we can start our request handling loop.
	go ts.handle()
	return ts, nil
}

type updateDiff struct {
	OldID   int64
	NewTask *Task
}

func (t *TaskStore) nextID() int64 {
	t.lastTaskID++
	return t.lastTaskID
}

// snapshot takes care of using the journaler to create a snapshot.
func (t *TaskStore) snapshot() error {
	if t.snapshotting {
		return ErrAlreadySnapshotting
	}
	t.snapshotting = true
	t.txnsSinceSnapshot = 0

	// First we make sure that the cache is flushed. We're still synchronous,
	// because we're in the main handler and no goroutines have been created.
	t.depleteCache(0)
	if len(t.tmpTasks)+len(t.delTasks) > 0 {
		panic("depleted cache in synchronous code, but not depleted. should never happen.")
	}

	// Now start snapshotting to the journaler.
	data := make(chan interface{}, 1)
	done := make(chan error, 1)
	snapresp := make(chan error, 1)
	go func() {
		done <- t.journaler.StartSnapshot(data, snapresp)
	}()

	go func() {
		var err error
		defer func() {
			// Notify of completion of this asynchronous snapshot.
			t.snapshotDoneChan <- err
		}()
		defer close(data)

		for _, task := range t.tasks {
			select {
			case data <- task:
				// Yay, data sent.
			case err = <-done:
				return // errors are sent out in defer
			case err = <-snapresp:
				return // errors are sent out in defer
			}
		}
	}()

	return nil
}

func (t *TaskStore) journalAppend(transaction []updateDiff) error {
	if t.journalChan != nil {
		// Opportunistic
		t.journalChan <- transaction
		return nil
	}
	// Strict
	return t.doAppend(transaction)
}

func (t *TaskStore) doAppend(transaction []updateDiff) error {
	if err := t.journaler.Append(transaction); err != nil {
		return err
	}
	t.txnsSinceSnapshot++
	return nil
}

// applyTransaction applies a series of mutations to the task store.
// Each element of the transaction contains information about the old task and
// the new task. Deletions are represented by a new nil task.
func (t *TaskStore) applyTransaction(transaction []updateDiff) error {
	if err := t.journalAppend(transaction); err != nil {
		return err
	}
	t.playTransaction(transaction, t.snapshotting)
	return nil
}

// playTransaction applies the diff (a series of mutations that should happen
// together) to the in-memory database. It does not journal.
func (t *TaskStore) playTransaction(tx []updateDiff, ro bool) {
	for _, diff := range tx {
		t.applySingleDiff(diff, ro)
	}
}

// depleteCache tries to move some of the elements in
// temporary structures into the main data area.
// Passing a number <= 0 indicates full depletion.
func (t *TaskStore) depleteCache(todo int) {
	if todo <= 0 {
		todo = len(t.tmpTasks) + len(t.delTasks)
	}
	for ; todo > 0; todo-- {
		switch {
		case len(t.tmpTasks) > 0:
			for id, task := range t.tmpTasks {
				t.tasks[id] = task
				delete(t.tmpTasks, id)
				break // just do one
			}
		case len(t.delTasks) > 0:
			for id := range t.delTasks {
				delete(t.tasks, id)
				delete(t.delTasks, id)
				break // just do one
			}
		default:
			todo = 0 // nothing left above
		}
	}
}

// applySingleDiff applies one of the updateDiff items in a transaction. If
// readonly is specified, it only writes to the temporary structures and skips
// the main tasks index so that it can remain constant while, e.g., written to
// a snapshot on disk.
func (t *TaskStore) applySingleDiff(diff updateDiff, readonly bool) {
	// If readonly, then we mutate only the temporary maps.
	// Regardless of that status, we always update the heaps.
	ot := t.getTask(diff.OldID)
	nt := diff.NewTask

	if ot != nil {
		delete(t.tmpTasks, ot.ID)
		t.heapPop(ot.Group, ot.ID)
		if readonly {
			t.delTasks[ot.ID] = true
		} else {
			delete(t.tasks, ot.ID)
		}
	}
	if nt != nil {
		if readonly {
			t.tmpTasks[nt.ID] = nt
		} else {
			t.tasks[nt.ID] = nt
		}
		t.heapPush(nt)
	}
}

// heapPop takes the specified task ID out of the heaps, and removes the group
// if it is now empty.
func (t *TaskStore) heapPop(group string, id int64) {
	h, ok := t.heaps[group]
	if !ok {
		return
	}
	h.PopByKey(id)
	if h.Len() == 0 {
		delete(t.heaps, group)
	}
}

// heapPush pushes something onto the heap for the task's group. Creates a new
// group if this one does not already exist.
func (t *TaskStore) heapPush(task *Task) {
	h, ok := t.heaps[task.Group]
	if !ok {
		h = keyheap.New()
		t.heaps[task.Group] = h
	}
	h.Push(task)
}

// canModify indicates whether a task is modifiable, meaning it's either
// expired or owned by the current client.
func canModify(now int64, clientID int32, task *Task) bool {
	return task.AT <= now || clientID == task.OwnerID
}

// missingDependencies returns the list of dependencies that are not in the store.
func (t *TaskStore) missingDependencies(deps []int64) []int64 {
	var missing []int64
	for _, id := range deps {
		if task := t.getTask(id); task == nil {
			missing = append(missing, id)
		}
	}
	return missing
}

// update performs (or attempts to perform) a batch task update.
func (t *TaskStore) update(up reqUpdate) ([]*Task, error) {
	uerr := UpdateError{}
	transaction := make([]updateDiff, len(up.Deletes)+len(up.Changes))
	if len(transaction) == 0 {
		uerr.Bugs = append(uerr.Bugs, fmt.Errorf("empty update requested"))
		return nil, uerr
	}

	// Check that the requested operation is allowed.
	// This means:
	// - All referenced task IDs must exist: dependencies, deletions, and updates.
	// - Updates and deletions must be modifying an unowned task, or a task owned by the requester.
	// - Additions are always OK.
	// - All of the above must be true *simultaneously* for any operation to be done.

	// Check that the dependencies are all around.
	if missing := t.missingDependencies(up.Depends); len(missing) > 0 {
		uerr.Depends = missing
	}

	now := Now()

	// Check that the deletions exist and are either owned by this client or expired, as well.
	// Also create transactions for these deletions.
	for i, id := range up.Deletes {
		task := t.getTask(id)
		if task == nil {
			uerr.Deletes = append(uerr.Deletes, id)
			continue
		}
		if !canModify(now, up.OwnerID, task) {
			uerr.Owned = append(uerr.Owned, task.ID)
			continue
		}
		// Make a deletion transaction.
		transaction[i] = updateDiff{id, nil}
	}

	for chgi, task := range up.Changes {
		i := chgi + len(up.Deletes)

		// Additions always OK.
		if task.ID <= 0 {
			if task.Group == "" {
				uerr.Bugs = append(uerr.Bugs, fmt.Errorf("update bug: adding task with empty task group not allowed: %v", task))
				continue
			}
			if !uerr.HasErrors() {
				transaction[i] = updateDiff{0, task.Copy()}
			}
			continue
		}
		// Everything else has to exist first.
		ot := t.getTask(task.ID)
		if ot == nil {
			uerr.Changes = append(uerr.Changes, task.ID)
			continue
		}
		if !canModify(now, up.OwnerID, ot) {
			uerr.Owned = append(uerr.Owned, ot.ID)
			continue
		}
		// Specifying a different (or any) group is meaningless in an update,
		// as this cannot be changed, so we just make sure that the update has
		// the same group as the existing task before creating the update entry.
		task.Group = ot.Group
		transaction[i] = updateDiff{task.ID, task.Copy()}
	}

	if uerr.HasErrors() {
		return nil, uerr
	}

	// Create new tasks for all non-deleted tasks, since we only get here without errors.
	// Also assign IDs and times as needed. Note that deletes are always in
	// front, so we start that far along in the transaction slice.
	newTasks := make([]*Task, len(up.Changes))
	for i, diff := range transaction[len(up.Deletes):] {
		nt := diff.NewTask
		newTasks[i] = nt
		if nt == nil {
			continue
		}
		// Assign IDs to all new tasks, and assign "now" to any that have no availability set.
		// Negative available time means "add the absolute value of this to now".
		nt.ID = t.nextID()
		if nt.AT <= 0 {
			nt.AT = now - nt.AT
		}
	}

	if err := t.applyTransaction(transaction); err != nil {
		uerr.Bugs = append(uerr.Bugs, err)
		return nil, uerr
	}

	return newTasks, nil
}

func (t *TaskStore) listGroup(lg reqListGroup) []*Task {
	h, ok := t.heaps[lg.Name]
	if !ok {
		// A non-existent group is actually not a problem. Groups are deleted
		// when empty and lazily created, so we allow them to simply come up
		// empty.
		return nil
	}
	limit := lg.Limit
	if limit <= 0 || limit > h.Len() {
		limit = h.Len()
	}
	var tasks []*Task
	if lg.AllowOwned {
		tasks = make([]*Task, limit)
		for i := range tasks {
			tasks[i] = h.PeekAt(i).(*Task)
		}
	} else {
		now := Now()
		tasks = make([]*Task, 0, limit)
		for i, found := 0, 0; i < h.Len() && found < limit; i++ {
			task := h.PeekAt(i).(*Task)
			if task.AT <= now {
				tasks = append(tasks, task)
				found++
			}
		}
	}
	return tasks
}

func (t *TaskStore) claim(claim reqClaim) (*Task, error) {
	now := Now()

	duration := claim.Duration
	if duration < 0 {
		duration = 0
	}

	// First check that all dependencies are available. If dependencies are
	// missing, we want an error even if there are no claims to be had in this
	// group. Dependency errors should never, ever pass silently.
	if missing := t.missingDependencies(claim.Depends); len(missing) > 0 {
		return nil, UpdateError{Depends: missing}
	}

	// Check that there are tasks ready to be claimed.
	h, ok := t.heaps[claim.Group]
	if !ok || h == nil || h.Len() == 0 {
		return nil, nil // not an error, just no tasks available
	}
	if task := h.Peek().(*Task); task.AT > now {
		return nil, nil // not an error, just no unowned tasks available
	}

	// We can proceed, so we create a random task update from the available
	// tasks.
	task := t.heaps[claim.Group].PopRandomConstrained(now).(*Task).Copy()
	// Create a mutated task that shares data and ID with this one, and
	// we'll request it to have these changes.
	reqtask := &Task{
		ID:      task.ID,
		OwnerID: claim.OwnerID,
		Group:   claim.Group,
		AT:      -duration,
		Data:    task.Data,
	}

	// Because claiming involves setting the owner and a future availability,
	// we update these acquired tasks.
	up := reqUpdate{
		OwnerID: claim.OwnerID,
		Changes: []*Task{reqtask},
		Depends: claim.Depends,
	}
	tasks, err := t.update(up)
	if err != nil {
		return nil, err
	}
	return tasks[0], nil
}

// reqUpdate contains the necessary fields for requesting an update to a
// set of tasks, including changes, deletions, and tasks on whose existence the
// update depends.
type reqUpdate struct {
	OwnerID int32
	Changes []*Task
	Deletes []int64
	Depends []int64
}

// reqListGroup is a query for up to Limit tasks in the given group name. If <=
// 0, all tasks are returned.
type reqListGroup struct {
	Name       string
	Limit      int
	AllowOwned bool
}

// reqClaim is a query for claiming one task from each of the specified groups.
type reqClaim struct {
	// The owner that is claiming the task.
	OwnerID int32

	// The Group is the name of the group from which a task is to be claimed.
	Group string

	// Duration is in nanoseconds. The task availability, if claimed, will
	// become now + Duration.
	Duration int64

	// Depends tells us which tasks must exist for this procedure to succeed.
	// This way you can turn task claiming on and off with separate signal
	// tasks.
	Depends []int64
}

// request wraps a query structure, and is used internally to handle the
// multi-channel request/response protocol.
type request struct {
	Val        interface{}
	ResultChan chan response
}

// response wraps a value by adding an error to it.
type response struct {
	Val interface{}
	Err error
}

// sendRequest sends val on the channel ch and waits for a response.
func (t *TaskStore) sendRequest(val interface{}, ch chan request) response {
	req := request{
		Val:        val,
		ResultChan: make(chan response, 1),
	}
	ch <- req
	return <-req.ResultChan
}

func (t *TaskStore) closed() bool {
	return t.journaler == nil
}

func (t *TaskStore) respondIfClosed(ch chan<- response) bool {
	if t.closed() {
		ch <- response{nil, ErrAlreadyClosed}
		return true
	}
	return false
}

// handle deals with all of the basic operations on the task store. All outside
// requests come through this single loop, which is part of the single-threaded
// access design enforcement.
func (t *TaskStore) handle() {
	idler := time.Tick(5 * time.Second)
	for {
		select {
		// Mutate cases
		case req := <-t.updateChan:
			if t.respondIfClosed(req.ResultChan) {
				continue
			}
			tasks, err := t.update(req.Val.(reqUpdate))
			if err == nil {
				// Successful txn: is it time to create a full snapshot?
				if t.txnsSinceSnapshot >= maxTxnsSinceSnapshot {
					err = t.snapshot()
					// Ignore error if it's just telling us that we're already snapshotting.
					if err == ErrAlreadySnapshotting {
						err = nil
					}
				}
			}
			req.ResultChan <- response{tasks, err}
		case req := <-t.claimChan:
			if t.respondIfClosed(req.ResultChan) {
				continue
			}
			tasks, err := t.claim(req.Val.(reqClaim))
			req.ResultChan <- response{tasks, err}
		case req := <-t.snapshotChan:
			if t.respondIfClosed(req.ResultChan) {
				continue
			}
			req.ResultChan <- response{nil, t.snapshot()}
		case req := <-t.closeChan:
			if t.respondIfClosed(req.ResultChan) {
				continue
			}
			err := t.journaler.Close()
			t.journaler = nil
			req.ResultChan <- response{nil, err}

		// Read cases
		case req := <-t.listGroupChan:
			tasks := t.listGroup(req.Val.(reqListGroup))
			req.ResultChan <- response{tasks, nil}
		case req := <-t.groupsChan:
			groups := make([]string, 0, len(t.heaps))
			for k := range t.heaps {
				groups = append(groups, k)
			}
			req.ResultChan <- response{groups, nil}
		case req := <-t.snapshottingChan:
			if t.respondIfClosed(req.ResultChan) {
				continue
			}
			req.ResultChan <- response{t.snapshotting, nil}
		case req := <-t.stringChan:
			strs := []string{"TaskStore:", "  groups:"}
			for name := range t.heaps {
				strs = append(strs, fmt.Sprintf("    %q", name))
			}
			strs = append(strs,
				fmt.Sprintf("  snapshotting: %v", t.snapshotting),
				fmt.Sprintf("  num tasks: %d", len(t.tasks)+len(t.tmpTasks)-len(t.delTasks)),
				fmt.Sprintf("  last task id: %d", t.lastTaskID))
			req.ResultChan <- response{strings.Join(strs, "\n"), nil}
		case req := <-t.allTasksChan:
			tasks := make([]*Task, 0, len(t.tasks))
			for _, v := range t.tasks {
				tasks = append(tasks, v)
			}
			sort.Slice(tasks, func(i, j int) bool {
				return tasks[i].ID < tasks[j].ID
			})
			req.ResultChan <- response{tasks, nil}
		case req := <-t.numTasksChan:
			// In this implementation, the number of tasks is always equivalent to the latest task ID.
			req.ResultChan <- response{t.lastTaskID, nil}
		case req := <-t.latestTaskIDChan:
			req.ResultChan <- response{t.lastTaskID, nil}
		case req := <-t.tasksChan:
			ids := req.Val.([]int64)
			tasks := make([]*Task, len(ids))
			for i, id := range ids {
				tasks[i] = t.getTask(id)
			}
			req.ResultChan <- response{tasks, nil}
		case req := <-t.isOpenChan:
			req.ResultChan <- response{!t.closed(), nil}

		// Internal cases.
		case <-idler:
			// The idler got a chance to tick. Trigger a short depletion.
			t.depleteCache(maxCacheDepletion)
		case err := <-t.snapshotDoneChan:
			if err != nil {
				log.Printf("snapshot failed: %v", err)
			}
			t.snapshotting = false
		case transaction := <-t.journalChan:
			if t.closed() {
				log.Printf("opportunistic transaction dropped because journal is closed:\n%v", transaction)
				continue
			}
			// Opportunistic journaling.
			t.doAppend(transaction)
		}
	}
}
