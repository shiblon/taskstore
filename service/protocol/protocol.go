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
	Error TaskResponseError `json:'errors'`
}

// A TaskResponseError is a slice of errors, one for each portion of a
// task request that failed.
type TaskResponseError []error

// Error satisfies the error interface, and produces a string representation of
// the errors within the response.
func (e TaskResponseError) Error() string {
	strs := make([]string, len(e))
	for i, err := range e {
		strs[i] = err.Error()
	}
	return fmt.Sprintf("task request errors:\n  - %s", strings.Join(strs, "\n  - "))
}
