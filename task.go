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
)

// Task is the atomic task unit. It contains a unique task, an owner ID, and an
// availability time (ms). The data is user-defined and can be basically anything.
//
// 0 (or less) is an invalid ID, and is used to indicate "please assign".
// A negative AT means "delete this task".
type Task struct {
	ID      int64  `json:"id"`
	OwnerID int32  `json:"ownerid"`
	Group   string `json:"group"`

	// The "Available Time": nanoseconds from the Epoch (UTC) when this task
	// becomes available. When used in requests, a value <= 0 is subtracted
	// from "right now" to generate a positive time value. Thus, 0 becomes
	// "now", and -time.Second (-1e9) becomes "1 second from now".
	AT int64 `json:"at"`

	// Data holds the data for this task.
	// If you want raw bytes, you'll need to encode them
	// somehow.
	Data []byte `json:"data"`
}

// NewTask creates a new task for this owner and group.
func NewTask(group string, data []byte) *Task {
	return &Task{
		Group: group,
		Data:  data,
	}
}

// Copy performs a shallow copy of this task.
func (t *Task) Copy() *Task {
	newTask := *t
	return &newTask
}

// String formats this task into a nice string value.
func (t *Task) String() string {
	return fmt.Sprintf("Task %d: g=%q o=%d t=%d d=%v", t.ID, t.Group, t.OwnerID, t.AT, t.Data)
}

// Priority returns an integer that can be used for heap ordering.
// In this case it's just the available time (ordering is from smallest to
// largest, or from oldest to newest).
func (t *Task) Priority() int64 {
	return t.AT
}

// Key returns the ID, to satisfy the keyheap.Item interface. This allows tasks to
// be found and removed from the middle of the heap.
func (t *Task) Key() int64 {
	return t.ID
}
