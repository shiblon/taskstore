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

// Definitions of protocol structures.
package protocol

import (
	"fmt"
	"strings"
)

type TaskInfo struct {
	ID    int64  `json:"id"`
	Group string `json:"group"`
	Data  string `json:"data"`

	// The TimeSpec, when positive, indicates an absolute timestamp in
	// nanoseconds since the epoch (UTC). When negative, its absolute value
	// will be added to the current time to create an appropriate timestamp.
	TimeSpec int64 `json:"duration"`

	// The owner, though present here, will always be ignored when trying to
	// perform an update. This is informational when obtaining tasks, not used
	// for changing them.
	OwnerID int32 `json:"ownerid"`
}

type UpdateRequest struct {
	ClientID int32      `json:"clientid"`
	Adds     []TaskInfo `json:"adds"`
	Updates  []TaskInfo `json:"updates"`
	Deletes  []int64    `json:"deletes"`
	Depends  []int64    `json:"depends"`
}

type ClaimRequest struct {
	ClientID int32   `json:"clientid"`
	Group    string  `json:"group"`
	Duration int64   `json:"duration"`
	Depends  []int64 `json:"depends"`
}

// A TaskResponse is used to return slices of tasks and errors. For example, if an UpdateRequest fails, the response will contain a list of reasons for the failure in the errors slice.
type TaskResponse struct {
	Tasks []TaskInfo        `json:'newtasks'`
	Error *TaskResponseError `json:'error'`
}

// A TaskResponseError is a slice of errors, one for each portion of a
// task request that failed.
type TaskResponseError struct {
	// Changes contains the list of tasks that were not present and could thus not be changed.
	Changes []int64 `json:'changes'`

	// Deletes contains the list of IDs that could not be deleted.
	Deletes []int64 `json:'deletes'`

	// Depends contains the list of IDs that were not present and caused the update to fail.
	Depends []int64 `json:'depends'`

	// Owned contains the list of IDs that were owned by another client and could not be changed.
	Owned []int64 `json:'owned'`

	// Bugs contains a list of errors representing caller precondition failures (bad inputs).
	Bugs []error `json:'bugs'`
}

func (te *TaskResponseError) HasDependencyErrors() bool {
	return len(te.Changes) > 0 || len(te.Deletes) > 0 || len(te.Depends) > 0 || len(te.Owned) > 0
}

func (te *TaskResponseError) HasBugs() bool {
	return len(te.Bugs) > 0
}

func (te *TaskResponseError) HasErrors() bool {
	return te.HasDependencyErrors() || te.HasBugs()
}

// Error returns an error string (and satisfies the Error interface).
func (te *TaskResponseError) Error() string {
	strs := []string{"update error:"}
	if len(te.Changes) > 0 {
		strs = append(strs, fmt.Sprintf("  Change IDs: %d", te.Changes))
	}
	if len(te.Deletes) > 0 {
		strs = append(strs, fmt.Sprintf("  Delete IDs: %d", te.Deletes))
	}
	if len(te.Depends) > 0 {
		strs = append(strs, fmt.Sprintf("  Depend IDs: %d", te.Depends))
	}
	if len(te.Owned) > 0 {
		strs = append(strs, fmt.Sprintf("  Owned IDs: %d", te.Owned))
	}
	if len(te.Bugs) > 0 {
		strs = append(strs, "  Bugs:")
		for _, e := range te.Bugs {
			strs = append(strs, fmt.Sprintf("    %v", e))
		}
	}
	return strings.Join(strs, "\n")
}
