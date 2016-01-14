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
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"testing/quick"
	"time"

	"entrogo.com/taskstore/journal"
)

func ExampleTaskStore() {
	// The task store is only the storage portion of a task queue. If you wish
	// to implement a service, you can easily do so using the primitives
	// provided. This example should give an idea of how you might do that.

	// To create a task store, specify a journal implementation. A real
	// implementation should use a lock of some kind to guarantee exclusivity.
	jr, err := journal.OpenDiskLog("/tmp/taskjournal")
	if err != nil {
		panic(fmt.Sprintf("could not create journal: %v", err))
	}

	// Then create the task store itself. You can create a "strict" store,
	// which requires that all transactions be flushed to the journal before
	// being committed to memory (and results returned), or "opportunistic",
	// which commits to memory and returns while letting journaling happen in
	// the background. If task execution is idempotent and it is always obvious
	// when to retry, you can get a speed benefit from opportunistic
	// journaling.
	store, err := OpenStrict(jr)
	if err != nil {
		fmt.Print("error opening taskstore: %v\n", err)
		return
	}
	defer store.Close()

	// To put a task into the store, call Update with the "add" parameter:
	add := []*Task{
		NewTask("groupname", []byte("task info, any string")),
	}

	// Every user of the task store needs a unique "OwnerID". When implementing
	// this as a service, the client library would likely assign this at
	// startup, so each process gets its own (and cannot change it). This is
	// one example of how to create an Owner ID.
	clientID := int32(rand.Int() ^ os.Getpid())

	// Request an update. Here you can add, modify, and delete multiple tasks
	// simultaneously. You can also specify a set of task IDs that must be
	// present (but will not be modified) for this operation to succeed.
	results, err := store.Update(clientID, add, nil, nil, nil)

	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// If successful, "results" will contain all of the newly-created tasks.
	// Note that even a task modification is relaly a task creation: it deletes
	// the old task and creates a new task with a new ID. IDs are guarnteed to
	// increase monotonically.
	fmt.Println(results)
}

func TestTaskStore_Update(t *testing.T) {
	fs := journal.NewMemFS("/myfs")
	jr, err := journal.OpenDiskLogInjectFS("/myfs", fs)
	if err != nil {
		t.Fatalf("failed to create journal: %v", err)
	}
	store, err := OpenStrict(jr)
	if err != nil {
		t.Fatalf("error opening taskstore: %v\n", err)
	}
	if !store.IsOpen() {
		t.Fatalf("task store not open after call to OpenStrict")
	}
	defer store.Close()

	var ownerID int32 = 11

	tasks := []*Task{
		NewTask("g1", []byte("hello there")),
		NewTask("g1", []byte("hi")),
		NewTask("g2", []byte("10")),
		NewTask("g2", []byte("5")),
		NewTask("g3", []byte("-")),
		NewTask("g3", []byte("_")),
	}

	added, err := store.Update(ownerID, tasks, nil, nil, nil)
	if err != nil {
		t.Fatalf("failed to add new tasks: %v", err)
	}

	now := Now()

	// Ensure that the tasks are exactly what we added, but with id values, etc.
	for i, task := range tasks {
		nt := added[i]
		if nt.ID <= 0 {
			t.Errorf("new task should have non-zero assigned ID, has %d", nt.ID)
		}
		if task.Group != nt.Group {
			t.Errorf("expected task group %q, got %q", task.Group, nt.Group)
		}
		if nt.OwnerID != ownerID {
			t.Errorf("expected owner ID %d, got %d", ownerID, nt.OwnerID)
		}
		if nt.AT > now {
			t.Errorf("new task has assigned available time in the future. expected t <= %d, got %d", now, nt.AT)
		} else if nt.AT <= 0 {
			t.Errorf("expected valid available time, got %d", nt.AT)
		}
		if string(nt.Data) != string(task.Data) {
			t.Errorf("expected task data to be %q, got %q", task.Data, nt.Data)
		}
	}

	groups := store.Groups()
	sort.Strings(groups)

	wantedGroups := []string{"g1", "g2", "g3"}

	if !eqStrings(wantedGroups, groups) {
		t.Fatalf("expected groups %v, got %v", wantedGroups, groups)
	}

	g1Tasks := store.ListGroup("g1", -1, true)
	if len(g1Tasks) != 2 {
		t.Errorf("g1 should have 2 tasks, has %v", g1Tasks)
	}

	// Try getting a non-existent group. Should come back nil.
	nongroupTasks := store.ListGroup("nongroup", -1, true)
	if nongroupTasks != nil {
		t.Errorf("nongroup should come back nil (no tasks, but not an error), came back %v", nongroupTasks)
	}

	// Now claim a task by setting its AT in the future by some number of milliseconds.
	t0 := added[0].Copy()
	t0.AT += int64(time.Minute)
	updated, err := store.Update(ownerID, nil, []*Task{t0}, nil, nil)
	if err != nil {
		t.Fatalf("failed to update task %v: %v", added[0], err)
	}
	if updated[0].ID <= added[0].ID {
		t.Errorf("expected updated task to have ID > original, but got %d <= %d", updated[0].ID, added[0].ID)
	}
	if updated[0].Group != added[0].Group {
		t.Errorf("expected updated task to have group %q, got %q", added[0].Group, updated[0].Group)
	}
	if updated[0].OwnerID != ownerID {
		t.Errorf("expected updated task to have owner ID %d, got %d", ownerID, updated[0].OwnerID)
	}
	if updated[0].AT-added[0].AT != int64(time.Minute) {
		t.Errorf("expected updated task to expire 1 minute later than before, but got a difference of %d", updated[0].AT-added[0].AT)
	}
	// Task is now owned, so it should not come back if we disallow owned tasks in a group listing.
	g1Available := store.ListGroup("g1", 0, false)
	if len(g1Available) > 1 {
		t.Errorf("expected 1 unowned task in g1, got %d", len(g1Available))
	}
	if g1Available[0].ID == updated[0].ID {
		t.Errorf("expected to get a task ID != %d (an unowned task), but got it anyway", updated[0].ID)
	}

	// This owner should be able to update its own future task.
	t0 = updated[0].Copy()
	t0.AT += int64(time.Second)
	updated2, err := store.Update(ownerID, nil, []*Task{t0}, nil, nil)
	if err != nil {
		t.Fatalf("couldn't update future task: %v", err)
	}
	if updated2[0].AT-updated[0].AT != int64(time.Second) {
		t.Errorf("expected 1-second increase in available time, got difference of %d milliseconds", updated2[0].AT-updated[0].AT)
	}

	// But another owner should not be able to touch it.
	t0 = updated2[0].Copy()
	t0.AT += 2 * int64(time.Second)
	_, err = store.Update(ownerID+1, nil, []*Task{t0}, nil, nil)
	if err == nil {
		t.Fatalf("owner %d should not succeed in updated task owned by %d", ownerID+1, ownerID)
	}
	uerr, ok := err.(UpdateError)
	if !ok {
		t.Fatalf("unexpected error type, could not convert to UpdateError: %#v", err)
	}
	if len(uerr.Owned) != 1 {
		t.Errorf("expected 1 error in UpdateError list, got %v", uerr)
	}
	if uerr.Owned[0] != t0.ID {
		t.Errorf("expected ownership error on task ID %d, got %v", t0.ID, uerr)
	}

	// Now try to update something that depends on an old task (our original
	// task, which has now been updated and is therefore no longer present).
	_, err = store.Update(ownerID, nil, []*Task{t0}, nil, []int64{tasks[0].ID})
	if err == nil {
		t.Fatalf("expected updated dependent on %d to fail, as that task should not be around", tasks[0].ID)
	}
	if err.(UpdateError).Depends[0] != tasks[0].ID {
		t.Fatalf("expected unmet dependency error for ID %d, got %v", tasks[0].ID, err)
	}

	// Try updating a task that we already updated.
	_, err = store.Update(ownerID, nil, []*Task{updated[0]}, nil, nil)
	if err == nil {
		t.Fatalf("expected to get an error when updating a task that was already updated")
	}
	if err.(UpdateError).Changes[0] != updated[0].ID {
		t.Fatalf("expected task not found error for ID %d, got %v", updated[0].ID, err)
	}

	// And now try deleting a task.
	updated3, err := store.Update(ownerID, nil, nil, []int64{added[2].ID}, nil)
	if err != nil {
		t.Fatalf("deletion of task %v failed: %v", added[2], err)
	}
	if len(updated3) != 0 {
		t.Fatalf("expected 0 updated tasks, got %v", updated3)
	}

	all := make(map[int64]*Task)
	for _, g := range store.Groups() {
		for _, t := range store.ListGroup(g, 0, true) {
			all[t.ID] = t
		}
	}

	expectedData := map[int64]string{
		2: "hi",
		4: "5",
		5: "-",
		6: "_",
		8: "hello there", // last to be updated, so it moved to the end
	}

	for id, data := range expectedData {
		if string(all[id].Data) != string(data) {
			t.Errorf("full dump: expected %q, got %q", data, all[id].Data)
		}
	}
}

func eqStrings(l1, l2 []string) bool {
	if len(l1) != len(l2) {
		return false
	}
	for i := range l1 {
		if l1[i] != l2[i] {
			return false
		}
	}
	return true
}

// ExampleTaskStore_tasks demonstrates the use of getting tasks by id.
func ExampleTaskStore_tasks() {

}

// ExampleTaskStore_mapReduce tests the taskstore by setting up a fake pipeline and
// working it for a while, just to make sure that things don't really hang up.
func ExampleTaskStore_mapReduce() {
	// We test the taskstore by creating a simple mapreduce pipeline.
	// This produces a word frequency histogram for the text below by doing the
	// following:
	//
	// - The lines of text create tasks, one for each line.
	// - Map goroutines consume those tasks, producing reduce groups.
	// - When all mapping is finished, one reduce task per group is created.
	// - Reduce goroutines consume reduce tasks, indicating which group to pull tasks from.
	// - They hold onto their reduce token, and so long as they own it, they
	// 	 perform reduce tasks and, when finished, push results into the result group.
	// - The results are finally read into a histogram.

	type Data struct {
		Key   string
		Count int
	}

	lines := []string{
		"The fundamental approach to parallel computing in a mapreduce environment",
		"is to think of computation as a multi-stage process, with a communication",
		"step in the middle. Input data is consumed in chunks by mappers. These",
		"mappers produce key/value pairs from their own data, and they are designed",
		"to do their work in isolation. Their computation does not depend on the",
		"computation of any of their peers. These key/value outputs are then grouped",
		"by key, and the reduce phase begins. All values corresponding to a",
		"particular key are processed together, producing a single summary output",
		"for that key. One example of a mapreduce is word counting. The input",
		"is a set of documents, the mappers produce word/count pairs, and the",
		"reducers compute the sum of all counts for each word, producing a word",
		"frequency histogram.",
	}

	numMappers := 3
	numReducers := 3
	maxSleepTime := 500 * int64(time.Millisecond)

	mainID := rand.Int31()

	// Create a taskstore backed by a fake in-memory journal.
	fs := journal.NewMemFS("/myfs")
	jr, err := journal.OpenDiskLogInjectFS("/myfs", fs)
	if err != nil {
		panic(fmt.Sprintf("failed to create journal: %v", err))
	}
	store, err := OpenStrict(jr)
	if err != nil {
		fmt.Printf("error opening task store: %v\n", err)
		return
	}
	defer store.Close()

	// And add all of the input lines.
	toAdd := make([]*Task, len(lines))
	for i, line := range lines {
		toAdd[i] = NewTask("map", []byte(line))
	}

	// Do the actual update.
	_, err = store.Update(mainID, toAdd, nil, nil, nil)
	if err != nil {
		panic(fmt.Sprintf("could not create task: %v", err))
	}

	// Start mapper workers.
	for i := 0; i < numMappers; i++ {
		go func() {
			mapperID := rand.Int31()
			for {
				// Get a task for ten seconds.
				maptask, err := store.Claim(mapperID, "map", int64(10 * time.Second), nil)
				if err != nil {
					panic(fmt.Sprintf("error retrieving tasks: %v", err))
				}
				if maptask == nil {
					time.Sleep(time.Duration(maxSleepTime))
					continue
				}
				// Now we have a map task. Split the data into words and emit reduce tasks for them.
				// The data is just a line from the text file.
				words := strings.Split(string(maptask.Data), " ")
				wm := make(map[string]int)
				for _, word := range words {
					word = strings.ToLower(word)
					word = strings.TrimSuffix(word, ".")
					word = strings.TrimSuffix(word, ",")
					wm[strings.ToLower(word)]++
				}
				// One task per word, each in its own group (the word's group)
				// This could just as easily be something in the filesystem,
				// and the reduce tasks would just point to them, but we're
				// using the task store because our data is small and because
				// we can.
				reduceTasks := make([]*Task, 0)
				for word, count := range wm {
					group := fmt.Sprintf("reduceword %s", word)
					reduceTasks = append(reduceTasks, NewTask(group, []byte(fmt.Sprintf("%d", count))))
				}
				delTasks := []int64{maptask.ID}
				_, err = store.Update(mapperID, reduceTasks, nil, delTasks, nil)
				if err != nil {
					panic(fmt.Sprintf("mapper failed: %v", err))
				}
			}
		}()
	}

	// Just wait for all map tasks to be deleted.
	for {
		tasks := store.ListGroup("map", 1, true)
		if len(tasks) == 0 {
			break
		}
		time.Sleep(time.Duration(rand.Int63n(maxSleepTime)+1))
	}

	// Now do reductions. To do this we list all of the reduceword groups and
	// create a task for each, then we start the reducers.
	//
	// Note that there are almost certainly better ways to do this, but this is
	// simple and good for demonstration purposes.
	//
	// Why create a task? Because tasks, unlike groups, can be exclusively
	// owned and used as dependencies in updates.
	groups := store.Groups()
	reduceTasks := make([]*Task, 0, len(groups))
	for _, g := range groups {
		if !strings.HasPrefix(g, "reduceword ") {
			continue
		}
		// Add the group name as a reduce task. A reducer will pick it up and
		// consume all tasks in the group.
		reduceTasks = append(reduceTasks, NewTask("reduce", []byte(g)))
	}

	_, err = store.Update(mainID, reduceTasks, nil, nil, nil)
	if err != nil {
		panic(fmt.Sprintf("failed to create reduce tasks: %v", err))
	}

	// Finally start the reducers.
	for i := 0; i < numReducers; i++ {
		go func() {
			reducerID := rand.Int31()
			for {
				grouptask, err := store.Claim(reducerID, "reduce", int64(30 * time.Second), nil)
				if err != nil {
					panic(fmt.Sprintf("failed to get reduce task: %v", err))
				}
				if grouptask == nil {
					time.Sleep(time.Duration(maxSleepTime))
					continue
				}
				gtdata := string(grouptask.Data)
				word := strings.SplitN(gtdata, " ", 2)[1]

				// No need to claim all of these tasks, just list them - the
				// main task is enough for claims, since we'll depend on it
				// before deleting these guys.
				tasks := store.ListGroup(gtdata, 0, true)
				delTasks := make([]int64, len(tasks)+1)
				sum := 0
				for i, task := range tasks {
					delTasks[i] = task.ID
					val, err := strconv.Atoi(string(task.Data))
					if err != nil {
						fmt.Printf("oops - weird value in task: %v\n", task)
						continue
					}
					sum += val
				}
				delTasks[len(delTasks)-1] = grouptask.ID
				outputTask := NewTask("output", []byte(fmt.Sprintf("%04d %s", sum, word)))

				// Now we delete all of the reduce tasks, including the one
				// that we own that points to the group, and add an output
				// task.
				_, err = store.Update(reducerID, []*Task{outputTask}, nil, delTasks, nil)
				if err != nil {
					panic(fmt.Sprintf("failed to delete reduce tasks and create output: %v", err))
				}

				// No need to signal anything - we just deleted the reduce
				// task. The main process can look for no tasks remaining.
			}
		}()
	}

	// Just look for all reduce tasks to be finished.
	for {
		tasks := store.ListGroup("reduce", 1, true)
		if len(tasks) == 0 {
			break
		}
		time.Sleep(time.Duration(rand.Int63n(maxSleepTime)+1))
	}

	// And now we have the finished output in the task store.
	outputTasks := store.ListGroup("output", 0, false)
	freqs := make([]string, len(outputTasks))
	for i, t := range outputTasks {
		freqs[i] = string(t.Data)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(freqs)))

	for i, f := range freqs {
		if i >= 10 {
			break
		}
		fmt.Println(f)
	}

	// Output:
	//
	// 0008 the
	// 0008 a
	// 0006 of
	// 0004 to
	// 0004 their
	// 0004 is
	// 0004 in
	// 0003 word
	// 0003 mappers
	// 0003 key
}

func TestTaskStore_Fuzz(t *testing.T) {
	const (
		Claim = iota
		Close
		ListGroup
		Groups
		NumTasks
		Tasks
		Update

		APIEnd
	)

	type Which struct {
		APIIndex int
		// Draw is a random value that can be used for variations on call
		// arguments,
		Draw int
	}

	config := &quick.Config{
		MaxCount: 10000,
		Values: func(values []reflect.Value, rand *rand.Rand) {
			// Just set up one journal. All bets are off if exclusivity is broken.
			fs := journal.NewMemFS("/tmp")
			values[0] = reflect.ValueOf(fs)

			jr, err := journal.OpenDiskLogInjectFS("/tmp", fs)
			if err != nil {
				t.Fatalf("failed to open journal: %v", err)
			}
			values[1] = reflect.ValueOf(jr)

			// Begin by opening the task store either in opportunistic or strict mode.
			values[2] = reflect.ValueOf(NewOpenCond(jr, rand.Intn(2) == 0))
			var apis []Which
			for i := 0; i < rand.Intn(20); i++ {
				apis = append(apis, Which{rand.Intn(APIEnd), rand.Int()})
			}
			values[3] = reflect.ValueOf(apis)

			// Generate a few groups to make a pool of.
			groupPool := []string{"g1", "g2", "g3", "g4"}
			values[4] = reflect.ValueOf(groupPool)

			// Generate some owner IDs to choose from.
			values[5] = reflect.ValueOf([]int32{100, 200, 300})

			// Generate a pool of tasks to use for adding to the task store.
			taskPool := make([]*Task, 30)
			dtype := reflect.TypeOf([]byte{})
			for i := range taskPool {
				dv, ok := quick.Value(dtype, rand)
				if !ok {
					panic("could not generate a random byte slice")
				}
				taskPool[i] = &Task{
					Group: groupPool[rand.Intn(len(groupPool))],
					AT:    Now() + rand.Int63n(int64(time.Second)) - 5 * int64(time.Millisecond),
					Data:  dv.Interface().([]byte),
				}
			}
			values[6] = reflect.ValueOf(taskPool)
		},
	}

	f := func(fs journal.FS, jr journal.Interface, o *OpenCond, which []Which, groupPool []string, ownerPool []int32, taskPool []*Task) bool {
		if err := o.Pre(nil); err != nil {
			fmt.Println("Error:", err)
			return false
		}
		o.Call()
		if err := o.Post(); err != nil {
			fmt.Println("Error:", err)
			return false
		}

		// Store is now open.
		store := o.RetStore

		if store == nil {
			fmt.Println("store not open!")
			return false
		}

		randOwner := func(draw int) int32 {
			return ownerPool[draw%len(ownerPool)]
		}

		randGroup := func(draw int) string {
			return groupPool[draw%len(groupPool)]
		}

		randTask := func(draw int) *Task {
			tasks := store.AllTasks()
			if len(tasks) == 0 {
				return nil
			}
			return tasks[draw%len(tasks)]
		}

		const (
			noGroup       = "nonexistent"
			noOwner int32 = 400
		)

		for _, w := range which {
			var cond Condition
			switch w.APIIndex {
			case Claim:
				r := w.Draw % 100
				group := noGroup
				owner := noOwner
				var depends []int64
				if r < 80 {
					group = randGroup(w.Draw)
				}
				if r < 90 {
					owner = randOwner(w.Draw)
				}
				if r < 30 {
					id := int64(r)
					if r < 25 {
						if t := randTask(w.Draw); t != nil {
							id = t.ID
						}
					}
					depends = append(depends, id)
				}
				var duration int64 = -10 * int64(time.Second)
				if r < 70 {
					duration = Now() + 5 * int64(time.Second)
				}
				cond = NewClaimCond(owner, group, duration, depends)
			case Close:
				cond = NewCloseCond()
			case ListGroup:
				r := w.Draw % 100
				limit := r - 1
				owned := r%2 == 0
				group := noGroup
				if r < 80 {
					group = randGroup(w.Draw)
				}
				cond = NewListGroupCond(group, limit, owned)
			case Groups:
				cond = NewGroupsCond()
			case NumTasks:
				cond = NewNumTasksCond()
			case Tasks:
				var ids []int64
				for i := w.Draw; i < w.Draw + (w.Draw % 7); i++ {
					if t := randTask(i); t != nil {
						ids = append(ids, t.ID)
					}
				}
				if w.Draw % 100 > 90 {
					// Add an unknown ID to exercise failure.
					ids = append(ids, int64(w.Draw % 1000))
				}
				cond = NewTasksCond(ids)
			case Update:
				r := w.Draw % 100
				var adds []*Task
				var changes []*Task
				var deletes []int64
				var depends []int64
				if r < 40 && len(taskPool) > 0 {
					n := w.Draw % 10
					if n > len(taskPool) {
						n = len(taskPool)
					}
					adds = taskPool[len(taskPool)-n:]
					taskPool = taskPool[:len(taskPool)-n]
				}
				if r < 30 {
					if r < 25 {
						if t := randTask(w.Draw); t != nil {
							depends = append(depends, t.ID)
						}
					}
					if t := randTask(w.Draw + 10); t != nil {
						depends = append(depends, t.ID)
					}
					if t := randTask(w.Draw + 7); t != nil {
						deletes = append(deletes, t.ID)
					}
					if len(depends) == 0 || r < 20 {
						// random ID, probably not in our store.
						depends = append(depends, int64(r))
						deletes = append(deletes, int64(r+5))
					}
				}
				if r > 15 {
					for i := 0; i < r%6+1; i++ {
						if t := randTask(w.Draw + 13); t != nil {
							t = t.Copy()
							t.Data = append(t.Data, byte(w.Draw&0xff))
							changes = append(changes, t)
						}
					}
					if r > 50 {
						// Update one that (probably) isn't there.
						if t := randTask(w.Draw + 3); t != nil {
							t = t.Copy()
							t.ID = int64(w.Draw)
							changes = append(changes, t)
						}
					}
				}
				cond = NewUpdateCond(randOwner(w.Draw), adds, changes, deletes, depends)
			default:
				panic(fmt.Sprintf("unknown condition indicator %v - should never happen", w))
			}
			if cond == nil {
				fmt.Printf("FIXME: blank condition for type %v\n", w)
				continue
			}
			if err := cond.Pre(store); err != nil {
				fmt.Println("Skipping:", err)
				continue
			}
			cond.Call()
			if err := cond.Post(); err != nil {
				fmt.Println("Error:", err)
				return false
			}
		}
		return true
	}

	if err := quick.Check(f, config); err != nil {
		t.Error(err)
	}
}
