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

// Package client implements a client for the HTTP taskstore service.
package client // import "entrogo.com/taskstore/service/client"

import (
	"bytes"
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"entrogo.com/taskstore/service/protocol"
)

var (
	clientID int32
)

func init() {
	buff := make([]byte, 4)
	n, err := crand.Read(buff)
	if err != nil {
		panic(err)
	}
	if n < len(buff) {
		panic(fmt.Sprintf("Failed to read %d bytes from crypto/rand.Reader. Only read %d bytes.", len(buff), n))
	}
	clientID = int32(buff[0] << 24) | int32(buff[1] << 16) | int32(buff[2] << 8) | int32(buff[3])
}

// ID returns the (hopefully unique) ID of this client instance.
func ID() int32 {
	return clientID
}

// The HTTPError type is returned when protocol operations succeed, but non-200 responses are returned.
type HTTPError error

// An HTTPClient provides access to a particular taskstore HTTP service, as specified by a URL.
type HTTPClient struct {
	baseURL string
	client  *http.Client
}

// NewHTTPClient creates an HTTPClient that attempts to connect to the given
// base URL for all operations.
func NewHTTPClient(baseURL string) *HTTPClient {
	return &HTTPClient{
		strings.TrimSuffix(baseURL, "/"),
		&http.Client{},
	}
}

func (h *HTTPClient) doRequest(r *http.Request) (*http.Response, error) {
	resp, err := h.client.Do(r)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("error status %q - also failed to get response body: %v", resp.Status, err)
		}
		return nil, fmt.Errorf("error status %q: %s", resp.Status, string(body))
	}
	return resp, nil
}

// Groups retrieves a list of group names from the task service.
func (h *HTTPClient) Groups() ([]string, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/%s", h.baseURL, "group"), nil)
	if err != nil {
		return nil, err
	}
	resp, err := h.doRequest(req)
	if err != nil {
		return nil, err
	}

	var groups []string
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&groups); err != nil {
		return nil, err
	}
	return groups, nil
}

// Task retrieves the task for the given ID, if it exists.
func (h *HTTPClient) Task(id int64) (*protocol.TaskInfo, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/task/%d", h.baseURL, id), nil)
	if err != nil {
		return nil, err
	}
	resp, err := h.doRequest(req)
	if err != nil {
		return nil, err
	}

	var task protocol.TaskInfo
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&task); err != nil {
		return nil, err
	}
	return &task, nil
}

// Tasks retrieves the tasks for the given list of IDs.
func (h *HTTPClient) Tasks(ids ...int64) ([]protocol.TaskInfo, error) {
	idstrs := make([]string, len(ids))
	for i, id := range ids {
		idstrs[i] = fmt.Sprintf("%d", id)
	}
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/tasks/%s", h.baseURL, strings.Join(idstrs, ",")), nil)
	if err != nil {
		return nil, err
	}
	resp, err := h.doRequest(req)
	if err != nil {
		return nil, err
	}

	var tasks []protocol.TaskInfo
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&tasks); err != nil {
		return nil, err
	}
	return tasks, nil
}

// Group retrieves the tasks for the given group. Optionally, a limit greater
// than zero indicates a maximum number of tasks that can be retrieved. If
// owned tasks should also be retrieved, set owned. Otherwise only tasks with
// an arrival time in the past will be returned. Note that allowing owned tasks
// does not discriminate by owner ID. All owned tasks will be allowed
// regardless of who owns them.
func (h *HTTPClient) Group(name string, limit int, owned bool) ([]protocol.TaskInfo, error) {
	ownint := 0
	if owned {
		ownint = 1
	}
	query := fmt.Sprintf("%s/group/%s?limit=%d&owned=%d", h.baseURL, name, limit, ownint)
	req, err := http.NewRequest("GET", query, nil)
	if err != nil {
		return nil, err
	}
	resp, err := h.doRequest(req)
	if err != nil {
		return nil, err
	}

	var tasks []protocol.TaskInfo
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&tasks); err != nil {
		return nil, err
	}
	return tasks, nil
}

// Update attempts to add, update, and delete the specified tasks, provided
// that all dependencies are met and the operation can be completed atomically
// and by the appropriate owner, etc.
//
// If successful, it returns a slice of tasks, appropriately updated (with new
// IDs). Otherwise, it returns an error. If the error is of type
// TaskResponseError, it means that the request succeeded, but the operation
// could not complete due to normal task store function: it is a slice of
// errors describing all of the failed constraints or dependencies that led to
// no update occurring.
func (h *HTTPClient) Update(adds, updates []protocol.TaskInfo, deletes, depends []int64) ([]protocol.TaskInfo, error) {
	request := protocol.UpdateRequest{
		ClientID: ID(),
		Adds:     adds,
		Updates:  updates,
		Deletes:  deletes,
		Depends:  depends,
	}
	mreq, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/update", h.baseURL), bytes.NewReader(mreq))
	if err != nil {
		return nil, err
	}
	resp, err := h.doRequest(req)
	if err != nil {
		return nil, err
	}

	var response protocol.TaskResponse
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&response); err != nil {
		return nil, err
	}
	if response.Error != nil && response.Error.HasErrors() {
		return nil, response.Error
	}
	return response.Tasks, nil
}

// Claim attempts to claim a task from the given group. If successful, the task
// returned will be leaesed for an additional duration nanoseconds before
// ownership expirees. The operation will only succeed if all task IDs in
// depends exist in the task store. A nil value indicates no dependencies.
//
// The task returned may be nil, indicating that no tasks were available to be
// claimed, but otherwise no errors occurred. If the error returned is of type
// TaskResponseError, then it will be a slice of errors, one for each
// unsatisifed task constraint (e.g., a missing dependency).
func (h *HTTPClient) Claim(group string, duration int64, depends []int64) (*protocol.TaskInfo, error) {
	request := protocol.ClaimRequest{
		ClientID: ID(),
		Group:    group,
		Duration: duration,
		Depends:  depends,
	}
	mreq, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/claim", h.baseURL), bytes.NewReader(mreq))
	if err != nil {
		return nil, err
	}
	resp, err := h.doRequest(req)
	if err != nil {
		return nil, err
	}

	var response protocol.TaskResponse
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&response); err != nil {
		return nil, err
	}
	if response.Error != nil {
		return nil, response.Error
	}
	// No tasks available is not an error. There just aren't any tasks.
	if len(response.Tasks) == 0 {
		return nil, nil
	}
	return &response.Tasks[0], nil
}
