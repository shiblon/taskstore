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
	"reflect"

	"entrogo.com/taskstore/journal"
)

// Pre/Post conditions for various API calls.
// These are used to represent what should happen when a call is made to an
// API function, depending on the state of the world before it happens.
type Condition interface {
	// Pre is called before calling the Call function. If it returns an error,
	// the Call function is not called because the precondition is not met.
	// You can pass in information to aid in providing state if needed.
	Pre(info interface{}) error

	// Call calls the API corresponding to this condition (e.g., Claim)
	Call()

	// Post is called after Call, and indicates whether the Call left things
	// in an appropriate state.
	Post() error
}

// A ClaimCond embodies the pre and post conditions for calling TaskStore.Claim.
type ClaimCond struct {
	Store *TaskStore

	PreDepend []*Task

	ArgOwner    int32
	ArgGroup    string
	ArgDuration int64
	ArgDepend   []int64

	RetTask *Task
	RetErr  error

	PreNow     int64
	PreOpen    bool
	PreTasks   []*Task
	PreUnowned []*Task
}

func NewClaimCond(owner int32, group string, duration int64, depends []int64) *ClaimCond {
	return &ClaimCond{
		ArgOwner:    owner,
		ArgGroup:    group,
		ArgDuration: duration,
		ArgDepend:   depends,
	}
}

func (c *ClaimCond) Pre(info interface{}) error {
	c.Store = info.(*TaskStore)
	c.PreNow = Now()
	c.PreOpen = c.Store.IsOpen()
	c.PreTasks = c.Store.ListGroup(c.ArgGroup, 0, true)
	c.PreUnowned = c.Store.ListGroup(c.ArgGroup, 0, false)
	c.PreDepend = c.Store.Tasks(c.ArgDepend)
	return nil
}

func (c *ClaimCond) Call() {
	c.RetTask, c.RetErr = c.Store.Claim(c.ArgOwner, c.ArgGroup, c.ArgDuration, c.ArgDepend)
}

func (c *ClaimCond) Post() error {
	if !c.PreOpen {
		if c.Store.IsOpen() {
			return fmt.Errorf("Claim Postcondition: store was not open, but it is now")
		}
		if c.RetErr == nil {
			return fmt.Errorf("Claim Postcondition: no error returned when claiming from a closed store")
		}
		tasks := c.Store.ListGroup(c.ArgGroup, 0, true)
		currentMap := make(map[int64]*Task)
		for _, t := range tasks {
			currentMap[t.ID] = t
		}
		previousMap := make(map[int64]struct{})
		for _, t := range c.PreTasks {
			previousMap[t.ID] = struct{}{}
			other, ok := currentMap[t.ID]
			if !ok {
				return fmt.Errorf("Claim Postcondition: store closed, but task %d disappeared", t.ID)
			}
			if !sameEssentialTask(t, other) {
				return fmt.Errorf("Claim Postcondition: store closed, but task has been altered:\nexpected\n%v\ngot\n%v", t, other)
			}
		}
		if len(previousMap) != len(currentMap) {
			var added []int64
			for k := range currentMap {
				if _, ok := previousMap[k]; !ok {
					added = append(added, k)
				}
			}
			return fmt.Errorf("Claim Postcondition: store closed, but new tasks appeared: %d", added)
		}
		return nil
	}

	// Check that failed dependencies cause a claim error
	var hasNil int64 = -1
	for i, task := range c.PreDepend {
		if task == nil {
			hasNil = c.ArgDepend[i]
			break
		}
	}
	if hasNil >= 0 {
		if c.RetErr == nil {
			return fmt.Errorf("Claim Postcondition: dependency %d missing, but claim succeeded", hasNil)
		}
		// Got an error when there were failed dependencies. That's good.
		return nil
	}

	now := Now()
	numUnowned := len(c.Store.ListGroup(c.ArgGroup, 0, false))
	if len(c.PreUnowned) == 0 {
		if numUnowned > 0 {
			return fmt.Errorf("Claim Postcondition: no tasks to claim, magically produced a claimable task")
		}
		if c.RetErr != nil {
			return fmt.Errorf("Claim Postcondition: should not be an error to claim no tasks when non exist, but got %v.\n", c.RetErr)
		}
		return nil
	}

	if c.RetTask == nil || c.RetErr != nil {
		return fmt.Errorf("Claim Postcondition: unowned tasks available, but none claimed: error %v\n", c.RetErr)
	}

	if c.RetTask.AT > now && numUnowned != len(c.PreUnowned)-1 {
		return fmt.Errorf("Claim Postcondition: unowned expected to change by -1, changed by %d", len(c.PreUnowned)-numUnowned)
	}

	if c.RetTask.OwnerID != c.ArgOwner {
		return fmt.Errorf("Claim Postcondition: owner not assigned properly to claimed task")
	}

	// TODO: check that the claimed task is actually one of the unowned tasks.

	return nil
}

// A CloseCond embodies the pre and post conditions for the Close call.
type CloseCond struct {
	Store   *TaskStore
	PreOpen bool
	RetErr  error
}

func NewCloseCond() *CloseCond {
	return &CloseCond{}
}

func (c *CloseCond) Pre(info interface{}) error {
	c.Store = info.(*TaskStore)
	c.PreOpen = c.Store.IsOpen()
	return nil
}

func (c *CloseCond) Call() {
	c.RetErr = c.Store.Close()
}

func (c *CloseCond) Post() error {
	postOpen := c.Store.IsOpen()
	if !c.PreOpen {
		if postOpen {
			return fmt.Errorf("Close Postcondition: magically opened from closed state.")
		}
		if c.RetErr == nil {
			return fmt.Errorf("Close Postcondition: closed store, but Close did not return an error.")
		}
		return nil
	}
	if c.RetErr != nil {
		return fmt.Errorf("Close Postcondition: closed an open store, but got an error: %v\n", c.RetErr)
	}
	if postOpen {
		return fmt.Errorf("Close Postcondition: failed to close store; still open.")
	}
	return nil
}

// A ListGroupCond embodies the pre and post conditions for listing tasks in a group.
type ListGroupCond struct {
	Store         *TaskStore
	PreNow        int64
	ArgGroup      string
	ArgLimit      int
	ArgAllowOwned bool
	RetTasks      []*Task
}

func NewListGroupCond(group string, limit int, allowOwned bool) *ListGroupCond {
	return &ListGroupCond{
		ArgGroup:      group,
		ArgLimit:      limit,
		ArgAllowOwned: allowOwned,
	}
}

func (c *ListGroupCond) Pre(info interface{}) error {
	c.Store = info.(*TaskStore)
	c.PreNow = Now()
	return nil
}

func (c *ListGroupCond) Call() {
	c.RetTasks = c.Store.ListGroup(c.ArgGroup, c.ArgLimit, c.ArgAllowOwned)
}

func (c *ListGroupCond) Post() error {
	if c.ArgLimit <= 0 {
		return nil // we can't really test this separately from itself.
	}
	if !c.ArgAllowOwned {
		for _, t := range c.RetTasks {
			// Not allowing owned, but got owned tasks anyway.
			if t.AT > c.PreNow {
				return fmt.Errorf("ListGroup Postcondition: got owned tasks when not asking for them.")
			}
		}
	}
	if len(c.RetTasks) > c.ArgLimit {
		return fmt.Errorf("ListGroup Postcondition: asked for max %d tasks, got more (%d).\n", c.ArgLimit, len(c.RetTasks))
	}
	return nil
}

// GroupsCond embodies the pre and post conditions for the store's Groups call.
type GroupsCond struct {
	Store     *TaskStore
	PreOpen   bool
	RetGroups []string
}

func NewGroupsCond() *GroupsCond {
	return &GroupsCond{}
}

func (c *GroupsCond) Pre(info interface{}) error {
	c.Store = info.(*TaskStore)
	c.PreOpen = c.Store.IsOpen()
	return nil
}

func (c *GroupsCond) Call() {
	c.RetGroups = c.Store.Groups()
}

func (c *GroupsCond) Post() error {
	if c.RetGroups == nil {
		return fmt.Errorf("Groups Postcondition: returned nil groups.")
	}
	return nil
}

// NumTasksCond embodies the pre and post conditions for the NumTasks call.
type NumTasksCond struct {
	Store  *TaskStore
	RetNum int
}

func NewNumTasksCond() *NumTasksCond {
	return &NumTasksCond{}
}

func (c *NumTasksCond) Pre(info interface{}) error {
	c.Store = info.(*TaskStore)
	return nil
}

func (c *NumTasksCond) Call() {
	c.RetNum = c.Store.NumTasks()
}

func (c *NumTasksCond) Post() error {
	if c.RetNum < 0 {
		return fmt.Errorf("NumTasks Postcondition: negative task num returned.")
	}
	return nil
}

// Tasks embodies the pre and post conditions for the Tasks call.
type TasksCond struct {
	Store    *TaskStore
	ArgIDs   []int64
	RetTasks []*Task
}

func NewTasksCond(ids []int64) *TasksCond {
	return &TasksCond{
		ArgIDs: ids,
	}
}

func (c *TasksCond) Pre(info interface{}) error {
	c.Store = info.(*TaskStore)
	return nil
}

func (c *TasksCond) Call() {
	c.RetTasks = c.Store.Tasks(c.ArgIDs)
}

func (c *TasksCond) Post() error {
	if len(c.RetTasks) > len(c.ArgIDs) {
		return fmt.Errorf("Tasks Postcondition: more tasks returned than requested.")
	}
	idmap := make(map[int64]struct{})
	for _, id := range c.ArgIDs {
		idmap[id] = struct{}{}
	}
	found := make(map[int64]struct{})
	for i, t := range c.RetTasks {
		if t != nil {
			if _, ok := idmap[t.ID]; !ok {
				return fmt.Errorf("Tasks Postcondition: returned task %d not in requested ID list %d.\n", t.ID, c.ArgIDs)
			}
			if t.ID != c.ArgIDs[i] {
				return fmt.Errorf("Tasks Postcondition: returned task %d not expected task %d.\n", t.ID, c.ArgIDs[i])
			}
		}
		found[c.ArgIDs[i]] = struct{}{}
	}
	if len(found) != len(idmap) {
		return fmt.Errorf("Tasks Postcondition: not all tasks accounted for: found %v, got %v\n", found, idmap)
	}
	return nil
}

// UpdateCond embodies the pre and post conditions for the Update call.
type UpdateCond struct {
	Store     *TaskStore
	PreOpen   bool
	PreChange []*Task
	PreDelete []*Task
	PreDepend []*Task
	ArgOwner  int32
	ArgAdd    []*Task
	ArgChange []*Task
	ArgDelete []int64
	ArgDepend []int64
	RetTasks  []*Task
	RetErr    error
	Now       int64
}

func NewUpdateCond(owner int32, add, change []*Task, del, dep []int64) *UpdateCond {
	return &UpdateCond{
		ArgOwner:  owner,
		ArgAdd:    add,
		ArgChange: change,
		ArgDelete: del,
		ArgDepend: dep,
	}
}

func (c *UpdateCond) Pre(info interface{}) error {
	c.Store = info.(*TaskStore)
	changeIDs := make([]int64, len(c.ArgChange))
	for i, t := range c.ArgChange {
		changeIDs[i] = t.ID
	}
	c.PreOpen = c.Store.IsOpen()
	if c.PreOpen {
		c.PreChange = c.Store.Tasks(changeIDs)
		c.PreDelete = c.Store.Tasks(c.ArgDelete)
		c.PreDepend = c.Store.Tasks(c.ArgDepend)
	}
	return nil
}

func (c *UpdateCond) Call() {
	c.Now = Now()
	c.RetTasks, c.RetErr = c.Store.Update(c.ArgOwner, c.ArgAdd, c.ArgChange, c.ArgDelete, c.ArgDepend)
}

func (c *UpdateCond) Post() error {
	if !c.PreOpen {
		if c.RetErr == nil {
			return fmt.Errorf("Update Postcondition: no error returned when updating a closed store.")
		}
		return nil
	}

	changeIDs := make(map[int64]struct{})
	for _, t := range c.ArgChange {
		changeIDs[t.ID] = struct{}{}
	}

	delIDs := make(map[int64]struct{})
	for _, id := range c.ArgDelete {
		delIDs[id] = struct{}{}
	}

	depIDs := make(map[int64]struct{})
	for _, id := range c.ArgDepend {
		depIDs[id] = struct{}{}
	}

	if c.RetErr != nil {
		retErr := c.RetErr.(UpdateError)
		if len(c.ArgAdd)+len(c.ArgChange)+len(c.ArgDelete) == 0 {
			// Empty updates are errors and are considered bugs.
			return nil
		}
		if c.existMissingDependencies() {
			// We expect an error if dependencies are missing.
			return nil
		}
		if c.existAlreadyOwned() {
			// We expect an error if changes or deletions are owned elsewhere.
			return nil
		}
		// Ensure that all of the IDs in the error condition are known.
		for _, id := range retErr.Changes {
			if _, ok := changeIDs[id]; !ok {
				return fmt.Errorf("Update Postcondition: error ID %d seen, but no such change ID requested: %v", id, retErr)
			}
		}
		for _, id := range retErr.Deletes {
			if _, ok := delIDs[id]; !ok {
				return fmt.Errorf("Update Postcondition: error ID %d seen, but no such delete ID requested: %v", id, retErr)
			}
		}
		for _, id := range retErr.Depends {
			if _, ok := depIDs[id]; !ok {
				return fmt.Errorf("Update Postcondition: error ID %d seen, but no such depend ID requested: %v", id, retErr)
			}
		}
		return fmt.Errorf("Update Postcondition: all tasks exist, none are owned by others, but still got an error: %v\n", c.RetErr)
	}

	if c.existMissingDependencies() {
		return fmt.Errorf("Update Postcondition: no error returned, but missing dependencies: %v\n", c)
	}
	if c.existAlreadyOwned() {
		return fmt.Errorf("Update Postcondition: no error returned, but modifications owned by others: %v\n", c)
	}

	if len(c.RetTasks) != len(c.ArgAdd)+len(c.ArgChange) {
		return fmt.Errorf("Update Postcondition: no error returned, but returned tasks not equal to sum of additions and changes: %v != %v + %v\n",
			len(c.RetTasks), len(c.ArgAdd), len(c.ArgChange))
	}

	newAdds := c.RetTasks[:len(c.ArgAdd)]
	for i, toAdd := range c.ArgAdd {
		added := newAdds[i]
		if added.OwnerID != c.ArgOwner {
			return fmt.Errorf("Update PostCondition: added task does not have the proper owner set: expected %v, got %v\n", c.ArgOwner, added.OwnerID)
		}
		if !sameEssentialTask(toAdd, added) {
			return fmt.Errorf("Update Postcondition: added task differs from requested add: expected\n%v\ngot\n%v\n", toAdd, added)
		}
		// TODO: In addition to the below, also ensure that the new ID is
		// bigger than any of the existing tasks we were looking at.
		if added.ID == 0 {
			return fmt.Errorf("Update Postcondition: added task has zero ID: %v\n", added)
		}
		expectedAT := toAdd.AT
		if toAdd.AT <= 0 {
			expectedAT = c.Now - toAdd.AT
		}
		if expectedAT > added.AT+5000 || expectedAT < added.AT-5000 {
			return fmt.Errorf("Update Postcondition: added task has weird AT: expected\n%v\ngot\n%v\n", toAdd, added)
		}
	}
	newChanges := c.RetTasks[len(c.ArgAdd):]
	for i, toChange := range c.ArgChange {
		changed := newChanges[i]
		if changed.OwnerID != c.ArgOwner {
			return fmt.Errorf("Update Postcondition: changed task does not have the proper owner set: expected %v, got %v\n", c.ArgOwner, changed.OwnerID)
		}
		if !sameEssentialTask(toChange, changed) {
			return fmt.Errorf("Update Postcondition: changed task differs from requested change: expected\n%v\ngot\n%v\n", toChange, changed)
		}
		if changed.ID <= toChange.ID {
			return fmt.Errorf("Update Postcondition: changed task should have strictly greater ID:\nrequest\n%v\nresponse\n%v\n", toChange, changed)
		}
		// Check that the old tasks are all gone.
		oldIDs := make([]int64, len(c.ArgChange))
		for i, t := range c.ArgChange {
			oldIDs[i] = t.ID
		}
		oldTasks := c.Store.Tasks(oldIDs)
		for _, t := range oldTasks {
			if t != nil {
				return fmt.Errorf("Update Postcondition: changed a task, but the old task is still present in the task store: %v\n", t)
			}
		}
	}

	deleted := c.Store.Tasks(c.ArgDelete)
	for i, t := range deleted {
		if t != nil {
			return fmt.Errorf("Update Postcondition: expected deleted tasks to disapper, but found %d still there.\n", c.ArgDelete[i])
		}
	}

	groups := c.Store.Groups()
	groupMap := make(map[string]struct{})
	for _, g := range groups {
		groupMap[g] = struct{}{}
	}
	for _, t := range c.ArgAdd {
		if _, ok := groupMap[t.Group]; !ok {
			return fmt.Errorf("Update Postcondition: added group %s to store, but that group is not present\n", t.Group)
		}
	}

	// TODO: check that now-empty groups are gone.
	return nil
}

// sameEssentialTask compares group and data to see if they are the same. It ignores AT, ID, and OwnerID.
func sameEssentialTask(t1, t2 *Task) bool {
	for i, d1 := range t1.Data {
		if d1 != t2.Data[i] {
			return false
		}
	}
	return t1.Group == t2.Group
}

func (c *UpdateCond) existAlreadyOwned() bool {
	for _, t := range c.PreChange {
		if t.OwnerID != c.ArgOwner && t.AT > c.Now {
			return true
		}
	}
	for _, t := range c.PreDelete {
		if t.OwnerID != c.ArgOwner && t.AT > c.Now {
			return true
		}
	}
	return false
}

func (c *UpdateCond) existMissingDependencies() bool {
	for _, t := range c.PreChange {
		if t == nil {
			return true
		}
	}
	for _, t := range c.PreDelete {
		if t == nil {
			return true
		}
	}
	for _, t := range c.PreDepend {
		if t == nil {
			return true
		}
	}
	return false
}

// OpenCond embodies the pre and post conditions for the Open{Opportunistic,Strict} call.
type OpenCond struct {
	Strict     bool
	PreBusy    bool
	ArgJournal journal.Interface
	RetStore   *TaskStore
	RetErr     error
}

func NewOpenCond(journal journal.Interface, strict bool) *OpenCond {
	return &OpenCond{
		ArgJournal: journal,
		Strict:     strict,
	}
}

func (c *OpenCond) Store() *TaskStore {
	return c.RetStore
}

func (c *OpenCond) Pre(info interface{}) error {
	if reflect.ValueOf(c.ArgJournal).IsNil() {
		return fmt.Errorf("Open Precondition: nil journal: %v\n", c.ArgJournal)
	}
	if !c.ArgJournal.IsOpen() {
		return fmt.Errorf("Open Precondition: journal is closed")
	}
	return nil
}

func (c *OpenCond) Call() {
	if c.Strict {
		c.RetStore, c.RetErr = OpenStrict(c.ArgJournal)
	} else {
		c.RetStore, c.RetErr = OpenOpportunistic(c.ArgJournal)
	}
}

func (c *OpenCond) Post() error {
	if c.RetErr != nil {
		if c.RetStore != nil {
			return fmt.Errorf("Open Postcondition: error returned, but store exists: %v\n", c.RetErr)
		}
		return nil
	}

	if !c.RetStore.IsOpen() {
		return fmt.Errorf("Open Postcondition: successful open, but store is not open: %v\n", c.RetStore)
	}
	if c.RetStore.IsStrict() != c.Strict {
		return fmt.Errorf("Open Postcondition: successful open, but strict (%v) should be %v: %v\n",
			c.RetStore.IsStrict(), c.Strict, c.RetStore)
	}
	// TODO: it would be nice to check that the journal and the contents of the
	// store actually match. But this is a lot more work, so we'll skip it for
	// now.
	return nil
}
