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

// A RESTful HTTP-based task service that uses the taskstore.
package main

// TODO:
// This should try to obtain a lock before opening the task store. There should
// never be more than one serice accessing a single journal directory. A nice
// way to handle this might be to use the chpst command to force a process to
// run only if a lock is acquired.

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"entrogo.com/taskstore"
	"entrogo.com/taskstore/journal"
	"entrogo.com/taskstore/service/protocol"
)

var (
	jdir            = flag.String("jdir", "", "directory to hold the task store journal - only one process should ever access it at a time.")
	port            = flag.Int("port", 8048, "port on which to listen for task requests")
	isOpportunistic = flag.Bool("opp", false, "turns on opportunistic journaling when true. This means that task updates are flushed to disk when possible. Leaving it strict means that task updates are flushed before given back to the caller.")
)

type StoreHandler struct {
	store *taskstore.TaskStore
}

func NewStoreHandler(dir string, opportunistic bool) (*StoreHandler, error) {
	journaler, err := journal.OpenDiskLog(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to create journal at %q: %v", dir, err)
	}

	newstoreFunc := taskstore.OpenStrict
	if opportunistic {
		newstoreFunc = taskstore.OpenOpportunistic
	}
	store, err := newstoreFunc(journaler)
	if err != nil {
		return nil, err
	}

	return &StoreHandler{store}, nil
}

// Groups returns a list of known groups in the task store.
func (s *StoreHandler) Groups(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		s.getGroups(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// Task returns a single task, specified by ID.
func (s *StoreHandler) Task(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		s.getTask(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// Tasks returns tasks for the given IDs, all that are available.
func (s *StoreHandler) Tasks(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		s.getTasks(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// Group returns a list of tasks for the specified group. It can be limited
// to a certain number, and can optionally allow owned tasks to be returned as
// well as unowned.
func (s *StoreHandler) Group(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		s.getGroup(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// Update attempts to update a set of tasks, including adds, updates, deletes,
// and depends. This is the core mutation call.
func (s *StoreHandler) Update(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		s.postUpdate(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// Claim accepts a group name with optional limit, duration, and dependencies.
func (s *StoreHandler) Claim(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		s.postClaim(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *StoreHandler) getGroups(w http.ResponseWriter, r *http.Request) {
	groups := s.store.Groups()
	out, err := json.Marshal(groups)
	if err != nil {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, fmt.Sprintf("Error forming json: %v", err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(out)
}

// getTask returns the specified Task info, if it exists in the store.
func (s *StoreHandler) getTask(w http.ResponseWriter, r *http.Request) {
	pieces := strings.Split(r.URL.Path, "/")
	if len(pieces) != 2 {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, fmt.Sprintf("invalid task request, expected /task/<id>, got %v\n", r.URL.Path))
		return
	}
	idstr := pieces[1]
	id, err := strconv.ParseInt(idstr, 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, fmt.Sprintf("invalid task request, expected /task/<numeric ID>, got %v\n", r.URL.Path))
		return
	}

	tasks := s.store.Tasks([]int64{id})
	out, jerr := json.Marshal(tasks[0])
	if jerr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, fmt.Sprintf("failed to marshal returned task (id %d) to json: %v\n", id, jerr))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(out)
}

// getTasks returns the specified tasks, if they exist in the store.
func (s *StoreHandler) getTasks(w http.ResponseWriter, r *http.Request) {
	pieces := strings.Split(r.URL.Path, "/")
	if len(pieces) != 2 {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, fmt.Sprintf("invalid tasks request, expected /tasks/<id,id,id,...>, got %v\n", r.URL.Path))
		return
	}
	var err error
	idstrs := strings.Split(pieces[1], ",")
	ids := make([]int64, len(idstrs))
	for i, str := range idstrs {
		ids[i], err = strconv.ParseInt(str, 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, fmt.Sprintf("invalid tasks request, non-numeric ID %q: %v\n", str, err))
			return
		}
	}

	tasks := s.store.Tasks(ids)
	out, jerr := json.Marshal(tasks)
	if jerr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, fmt.Sprintf("failed to marshal returned tasks (ids %d) to json: %v\n", ids, jerr))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(out)
}

// getGroup returns a list of tasks for a provided group.
func (s *StoreHandler) getGroup(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	pieces := strings.Split(r.URL.Path, "/")
	if len(pieces) != 2 {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, fmt.Sprintf("invalid group request, expected /group/<groupname>, got %v\n", r.URL.Path))
		return
	}
	name := pieces[1]

	var err error
	limit := 0
	allowOwned := false
	if lstr := r.Form.Get("limit"); lstr != "" {
		limit, err = strconv.Atoi(lstr)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, fmt.Sprintf("invalid limit parameter %q, expected a number\n", lstr))
			return
		}
	}
	if astr := r.Form.Get("owned"); astr != "" {
		switch strings.ToLower(astr) {
		case "yes", "true", "1":
			allowOwned = true
		case "no", "false", "0":
			allowOwned = false
		default:
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, fmt.Sprintf("invalid owned parameter %q, expected a boolean\n", astr))
			return
		}
	}
	tasks := s.store.ListGroup(name, limit, allowOwned)
	out, err := json.Marshal(tasks)
	if err != nil {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusInternalServerError)
		log.Printf("failed json encoding of task list: %v", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(out)
}

// postClaim is called when a task is to be claimed.
func (s *StoreHandler) postClaim(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	claimstr := r.Form.Get("claim")
	if claimstr == "" {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, "no parameters provided for task claim")
		return
	}

	var claim protocol.ClaimRequest
	err := json.Unmarshal([]byte(claimstr), &claim)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, fmt.Sprintf("failed to decode json claim %v: %q", claimstr, err))
		return
	}

	task, err := s.store.Claim(claim.ClientID, claim.Group, claim.Duration, claim.Depends)
	if err != nil {
		uerr := err.(taskstore.UpdateError)
		response := protocol.TaskResponse{
			Error: newTaskResponseError(uerr),
		}
		out, jerr := json.Marshal(response)
		if jerr != nil {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, fmt.Sprintf("failed to marshal failed claim response: %v", jerr))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(out)
		return
	}
	response := protocol.TaskResponse{}
	if task != nil {
		response.Tasks = []protocol.TaskInfo{
			protocol.TaskInfo{
				ID:       task.ID,
				Group:    task.Group,
				Data:     string(task.Data),
				TimeSpec: task.AT,
				OwnerID:  task.OwnerID,
			},
		}
	}
	out, jerr := json.Marshal(response)
	if jerr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, fmt.Sprintf("failed to marshal successful claim response: %v", jerr))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(out)
	return
}

// postUpdate is called when a task updated is attempted. It calls taskstore.TaskStore.Update.
func (s *StoreHandler) postUpdate(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	updatestr := r.Form.Get("update")
	if updatestr == "" {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, "no data provided for task update")
		return
	}

	var update protocol.UpdateRequest
	err := json.Unmarshal([]byte(updatestr), &update)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, fmt.Sprintf("failed to decode json update %v: %v", updatestr, err))
		return
	}

	// We have an update request. Reformat to proper tasks, etc., as expected by the taskstore.
	adds := make([]*taskstore.Task, len(update.Adds))
	updates := make([]*taskstore.Task, len(update.Updates))

	now := taskstore.Now()

	for i, a := range update.Adds {
		ts := a.TimeSpec
		if ts <= 0 {
			ts = now - ts
		}
		adds[i] = &taskstore.Task{
			ID:    0,
			Group: a.Group,
			AT:    ts,
			Data:  []byte(a.Data),
		}
	}

	for i, u := range update.Updates {
		ts := u.TimeSpec
		if ts <= 0 {
			ts = now - ts
		}
		updates[i] = &taskstore.Task{
			ID:    u.ID,
			Group: u.Group,
			AT:    ts,
			Data:  []byte(u.Data),
		}
	}

	// Perform the actual update. Finally.
	newtasks, err := s.store.Update(update.ClientID, adds, updates, update.Deletes, update.Depends)
	if err != nil {
		out, jerr := json.Marshal(err)
		if jerr != nil {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, fmt.Sprintf("failed to marshal the json encoding error that follows: %v\n%v", jerr, err))
			return
		}
		// update errors are fine and expected. We just return an error object in that case.
		uerr := err.(taskstore.UpdateError)
		response := protocol.TaskResponse{
			Error: newTaskResponseError(uerr),
		}
		out, jerr = json.Marshal(response)
		if jerr != nil {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, fmt.Sprintf("failed to marshal failed update response: %v", jerr))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(out)
		return
	}

	outtasks := make([]protocol.TaskInfo, len(newtasks))
	for i, t := range newtasks {
		outtasks[i] = protocol.TaskInfo{
			ID:       t.ID,
			Group:    t.Group,
			Data:     string(t.Data),
			TimeSpec: t.AT,
			OwnerID:  t.OwnerID,
		}
	}

	response := protocol.TaskResponse{
		Tasks: outtasks,
	}
	out, jerr := json.Marshal(response)
	if jerr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, fmt.Sprintf("failed to marshal successful update response %v", jerr))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(out)
}

func newTaskResponseError(ue taskstore.UpdateError) *protocol.TaskResponseError {
	te := &protocol.TaskResponseError{
		Changes: make([]int64, len(ue.Changes)),
		Deletes: make([]int64, len(ue.Deletes)),
		Depends: make([]int64, len(ue.Depends)),
		Owned: make([]int64, len(ue.Owned)),
		Bugs: make([]error, len(ue.Bugs)),
	}
	copy(te.Changes, ue.Changes)
	copy(te.Deletes, ue.Deletes)
	copy(te.Depends, ue.Depends)
	copy(te.Owned, ue.Owned)
	copy(te.Bugs, ue.Bugs)
	return te
}

func main() {
	flag.Parse()

	if *jdir == "" {
		fmt.Println("please specify a journal directory via -jdir")
		os.Exit(-1)
	}

	store, err := NewStoreHandler(*jdir, *isOpportunistic)
	if err != nil {
		fmt.Println("failed to create a task store: %v", err)
		os.Exit(-1)
	}

	http.HandleFunc("/test/", func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		w.Header().Set("Content-Type", "text/plain")
		io.WriteString(w, fmt.Sprintf("%s\n", r.URL.Path))
		io.WriteString(w, fmt.Sprintf("%#v\n", r.Form))
		io.WriteString(w, fmt.Sprintf("Test\n"))
	})

	http.HandleFunc("/groups", store.Groups) // GET retrieves a list of groups.
	http.HandleFunc("/task/", store.Task)    // GET retrieves the task, specified by numeric ID.
	http.HandleFunc("/tasks/", store.Tasks)  // GET retrieves a list of comma-separated tasks by ID.
	http.HandleFunc("/group/", store.Group)  // GET retrieves tasks for the given group.

	http.HandleFunc("/update", store.Update) // POST updates the specified tasks.
	http.HandleFunc("/claim", store.Claim)   // POST takes a required group name

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
}
