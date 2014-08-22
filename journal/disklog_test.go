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

package journal

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"testing/quick"
	"time"
)

var (
	logBuffer bytes.Buffer
)

func init() {
	// Override the Logf function to write to a buffer instead of stdout.
	Logf = func(fstr string, vals ...interface{}) {
		now := time.Now().Format("2006/01/02 15:05:05")
		s := fmt.Sprintf("%s %s", now, fmt.Sprintf(fstr, vals))
		if !strings.HasSuffix(s, "\n") {
			s += "\n"
		}
		logBuffer.WriteString(s)
	}
}

func ExampleDiskLog() {
	// Open up the log in directory "/tmp/disklog". Will create an error if it does not exist.
	fs := NewMemFS("/tmp/disklog")
	journal, err := OpenDiskLogInjectFS("/tmp/disklog", fs)
	if err != nil {
		fmt.Println(err)
		return
	}

	// Data type can be anything. Here we're adding integers one at a time. We
	// could also add the entire list at once, since it just gets gob-encoded.
	data := []int{2, 3, 5, 7, 11, 13}
	for _, d := range data {
		if err := journal.Append(d); err != nil {
			fmt.Printf("Failed to append %v: %v\n", d, err)
		}
	}
	// We didn't write enough to trigger a rotation, so everything should be in
	// the current journal. Let's see if we get it back.
	decoder, err := journal.JournalDecoder()
	if err != nil {
		fmt.Printf("error getting decoder: %v\n", err)
		return
	}
	vals := make([]int, 0)
	val := -1
	for {
		err := decoder.Decode(&val)
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("Error:", vals)
			fmt.Printf("failed to decode next item in journal: %v\n", err)
			return
		}
		vals = append(vals, val)
	}
	fmt.Println("Success", vals)

	// Output:
	//
	// Success [2 3 5 7 11 13]
}

type record struct {
	Ival int
	Sval string
	Fval float64
	Cval struct {
		X int
		Y int
	}
}

func (r record) String() string {
	strs := []string{
		fmt.Sprintf("\tIval: %d", r.Ival),
		fmt.Sprintf("\tSval: %q", r.Sval),
		fmt.Sprintf("\tFval: %f", r.Fval),
		fmt.Sprintf("\tCval: %v", r.Cval),
	}
	return strings.Join(strs, "\n")
}

func (r record) Equal(other record) bool {
	return (r.Ival == other.Ival &&
		r.Sval == other.Sval &&
		r.Fval == other.Fval &&
		r.Cval.X == other.Cval.X &&
		r.Cval.Y == other.Cval.Y)
}

func TestDiskLog(t *testing.T) {
	config := &quick.Config{
		Rand: rand.New(rand.NewSource(time.Now().Unix())),
	}

	f := func(records []record) bool {
		// Open up the log in directory "/tmp/disklog". Will create an error if it does not exist.
		fs := NewMemFS("/tmp/disklog")
		journal, err := OpenDiskLogInjectFS("/tmp/disklog", fs)
		if err != nil {
			fmt.Println(err)
			return false
		}

		// Take the supplied random records, apply them to the journal, and
		// ensure that we get back what we put in.
		for _, r := range records {
			err := journal.Append(r)
			if err != nil {
				fmt.Println(err)
				return false
			}
		}
		err = journal.Close()
		if err != nil {
			fmt.Println(err)
			return false
		}

		// Now decode. Open the journal again and get the values out.
		journal2, err := OpenDiskLogInjectFS("/tmp/disklog", fs)
		if err != nil {
			fmt.Println(err)
			return false
		}

		decoder, err := journal2.JournalDecoder()
		if err != nil {
			fmt.Println(err)
			return false
		}
		var i int
		for {
			var r record
			err := decoder.Decode(&r)
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Printf("decode failure at record %d: %v\n", err)
				return false
			}
			if !r.Equal(records[i]) {
				fmt.Printf("Bad record retrieved at %d:\nExpected\n%v\nActual\n%v\n", i, records[i], r)
				return false
			}
			i++
		}
		return true
	}

	if err := quick.Check(f, config); err != nil {
		t.Error(err)
	}
}

type JournalDirName string

func (JournalDirName) Generate(rand *rand.Rand, size int) reflect.Value {
	username := "UnknownUser"
	if u, err := user.Current(); err == nil {
		username = u.Username
	}
	dir := fmt.Sprintf("/tmp/test/%s/taskstore/%d-%d-%d", username, os.Getpid(), time.Now().Unix(), rand.Int())

	return reflect.ValueOf(JournalDirName(dir))
}

func TestDiskLog_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	type result struct {
		id  int
		err error
	}

	type idrec struct {
		ID int
		R  *record
	}

	config := &quick.Config{
		Rand: rand.New(rand.NewSource(time.Now().Unix())),
		Values: func(values []reflect.Value, rand *rand.Rand) {
			dir := JournalDirName("/tmp/test/INVALID-taskstore")
			if dval, ok := quick.Value(reflect.TypeOf(JournalDirName("")), rand); ok {
				dir = dval.Interface().(JournalDirName)
			}
			values[0] = reflect.ValueOf(dir)
			recs, ok := quick.Value(reflect.TypeOf([]record{}), rand)
			if !ok {
				panic("failed to create value for list of records")
			}
			values[1] = recs
			l := recs.Len()
			if l > 0 {
				values[2] = reflect.ValueOf(rand.Intn(values[0].Len()))
			} else {
				values[2] = reflect.ValueOf(0)
			}
		},
	}

	f := func(dir JournalDirName, records []record, rotateIndex int) bool {
		// Make sure the directory exists
		// fs := OSFS{}
		fs := NewMemFS(string(dir))
		if err := fs.MkdirAll(string(dir), 0770); err != nil {
			fmt.Println(err)
			return false
		}
		defer func() {
			if !strings.HasPrefix(string(dir), "/tmp/test/") {
				fmt.Println("failed to remove directory - not in /tmp/test - considered dangerous")
				return
			}
			if err := fs.RemoveAll(string(dir)); err != nil {
				fmt.Println(err)
			}
		}()

		journal, err := OpenDiskLogInjectFS(string(dir), fs)
		if err != nil {
			fmt.Println(err)
			return false
		}

		results := make(chan result, len(records))

		// Take the supplied random records, apply them to the journal, and
		// ensure that we get back what we put in. Except this time, we do each
		// one in a new goroutine, then test that there is a serialization that
		// actually works.
		for i := range records {
			r := idrec{i, &records[i]}
			go func() {
				out := result{r.ID, nil}
				defer func() { results <- out }()

				if out.err = journal.Append(r); out.err != nil {
					return
				}
				if r.ID == rotateIndex {
					if out.err = journal.Rotate(); out.err != nil {
						return
					}
				}
			}()
		}

		for i := 0; i < len(records); i++ {
			res := <-results
			if res.err != nil {
				fmt.Println(res.id, res.err)
				return false
			}
		}

		err = journal.Close()
		if err != nil {
			fmt.Println(err)
			return false
		}

		// Now decode. Open the journal again and get the values out, checking
		// that there exists a serialization that gives us these results (no
		// corruption, all records reappear).
		journal2, err := OpenDiskLogInjectFS(string(dir), fs)
		if err != nil {
			fmt.Println(err)
			return false
		}

		remaining := make(map[int]struct{})
		for i := range records {
			remaining[i] = struct{}{}
		}

		decoder, err := journal2.JournalDecoder()
		if err != nil {
			fmt.Println(err)
			return false
		}
		for {
			var r idrec
			err := decoder.Decode(&r)
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Printf("decode failure: %v\n", err)
				return false
			}
			if _, ok := remaining[r.ID]; !ok {
				fmt.Printf("got record %d twice\n", r.ID)
				return false
			}
			delete(remaining, r.ID)

			if !r.R.Equal(records[r.ID]) {
				fmt.Printf("Bad record retrieved at %d:\nExpected\n%v\nActual\n%v\n", r.ID, records[r.ID], r.R)
				return false
			}
		}
		if len(remaining) != 0 {
			fmt.Printf("not all records retrieved: %v\n", remaining)
			return false
		}
		return true
	}

	if err := quick.Check(f, config); err != nil {
		t.Error(err)
	}
}

func TestDiskLog_Rotate(t *testing.T) {
	fs := NewMemFS("/tmp/disklog")
	journal, err := OpenDiskLogInjectFS("/tmp/disklog", fs)
	if err != nil {
		t.Fatalf("failed to create memfs disklog: %v", err)
	}

	// Add data, rotate, then add more data

	beforedata := []int{2, 3, 5, 7, 11}
	afterdata := []int{13, 17, 23}
	for _, d := range beforedata {
		if err := journal.Append(d); err != nil {
			t.Fatalf("failed to append data: %v", err)
		}
	}
	journal.Rotate()
	for _, d := range afterdata {
		if err := journal.Append(d); err != nil {
			t.Fatalf("failed to append data: %v", err)
		}
	}

	// Pull all of the data out. We should get all of it in order.
	decoder, err := journal.JournalDecoder()
	if err != nil {
		t.Fatalf("failed to create a decoder: %v", err)
	}
	var val int
	vals := make([]int, 0)
	for {
		err := decoder.Decode(&val)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("decode failed: %v", err)
		}
		vals = append(vals, val)
	}

	alldata := append(beforedata, afterdata...)
	good := true
	if len(alldata) != len(vals) {
		good = false
	} else {
		for i, d := range vals {
			if d != alldata[i] {
				good = false
				break
			}
		}
	}
	if !good {
		t.Errorf("expected\n%v\ngot\n%v", alldata, vals)
	}

	// finally, check that we have two different files, one open and one frozen.
	working, err := fs.FindMatching("/tmp/disklog/*.log.working")
	if err != nil {
		t.Fatalf("error getting working files: %v", err)
	}
	frozen, err := fs.FindMatching("/tmp/disklog/*.log")
	if err != nil {
		t.Fatalf("error getting frozen files: %v", err)
	}

	if len(working) != 1 {
		t.Fatalf("expected %d working file(s), found %d", 1, len(working))
	}
	if len(frozen) != 1 {
		t.Fatalf("expected %d frozen file(s), found %d", 1, len(frozen))
	}

	workbase := filepath.Base(working[0])
	frozenbase := filepath.Base(frozen[0])

	workts, err := TSFromName(workbase)
	if err != nil {
		t.Fatalf("can't get timestamp from working name %q: %v", workbase, err)
	}
	frozents, err := TSFromName(frozenbase)
	if err != nil {
		t.Fatalf("can't get timestamp from frozen name %q: %v", frozenbase, err)
	}

	if workts <= frozents {
		t.Fatalf(
			"working logs should always be newer than frozen logs. "+
				"Got working=%d, frozen=%d from\n%q\n%q", workts, frozents, workbase, frozenbase)
	}
}

func TestDiskLog_Decode_Corrupt(t *testing.T) {
	// Open up the log in directory "/tmp/disklog". Will create an error if it does not exist.
	fs := NewMemFS("/tmp/disklog")
	journal, err := OpenDiskLogInjectFS("/tmp/disklog", fs)
	if err != nil {
		t.Fatalf("failed to open /tmp/disklog: %v\n", err)
	}

	data := []int{2, 3, 5, 7, 11, 13}
	for _, d := range data {
		if err := journal.Append(d); err != nil {
			t.Fatalf("failed to append %v: %v\n", d, err)
		}
	}

	working, err := fs.FindMatching("/tmp/disklog/*.log.working")
	if err != nil {
		t.Fatalf("could not match: %v", err)
	}
	if len(working) == 0 {
		t.Fatalf("no working files found")
	}

	// Write bogus data at the end, but not 0 padding:
	fs.files[working[0]].Write([]byte{2})

	// And try to decode, including the last bogus record.
	// It should be ignored by the decoder.
	decoder, err := journal.JournalDecoder()
	if err != nil {
		t.Fatalf("error getting decoder: %v\n", err)
		return
	}
	vals := make([]int, 0)
	val := -1
	for {
		err := decoder.Decode(&val)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Error("Error:", vals)
			t.Fatalf("failed to decode next item in journal: %v\n", err)
		}
		vals = append(vals, val)
	}

	// Then ensure that we still have all the data AND that we got the unexpected EOF.
	for i, d := range vals {
		if d != data[i] {
			t.Fatalf("expected\n\t%v\ngot\n\t%v", data, vals)
		}
	}
	// Check for the unexpected EOF error.
	if re := regexp.MustCompile(` journal file \[.*\] has an unexpected EOF\n`); !re.Match(logBuffer.Bytes()) {
		t.Fatalf("expected 'unexpected EOF' error, did not see it. Log follows:\n%q", logBuffer.String())
	}
}

func TestDiskLog_Snapshot(t *testing.T) {
	type dtype struct {
		S string
		I int
	}

	f := func(dir JournalDirName, data []dtype, appendData []dtype) bool {
		// fs := NewMemFS("/myfs")
		fs := OSFS{}
		// Make sure the directory exists
		if err := fs.MkdirAll(string(dir), 0770); err != nil {
			fmt.Println(err)
			return false
		}
		journal, err := OpenDiskLogInjectFS(string(dir), fs)
		if err != nil {
			fmt.Printf("failed to create journal: %v", err)
			return false
		}

		// Set off some appends before we request a snapshot. The snapshot
		// should pause them while it rotates the log, then allow them to
		// simply continue. We do this testing to ensure that we don't have
		// crashes due to race conditions.
		appendErrorChan := make(chan error, 1)
		go func() {
			defer close(appendErrorChan)
			for _, d := range appendData {
				if err := journal.Append(d); err != nil {
					appendErrorChan <- err
					return
				}
			}
		}()

		recs := make(chan interface{}, 1)
		resp := make(chan error, 1)
		go func() {
			defer close(recs)
			for _, d := range data {
				recs <- d
			}
		}()
		if err := journal.StartSnapshot(recs, resp); err != nil {
			fmt.Printf("error starting snapshot: %v", err)
			return false
		}

		if err := <-resp; err != nil {
			fmt.Printf("error in response to snapshot request: %v", err)
			return false
		}

		// Now the snapshot is complete. Ensure that it is there.
		names, err := fs.FindMatching(path.Join(string(dir), "*.*.snapshot"))
		if len(names) != 1 {
			fmt.Printf("expected 1 snapshot, found %d: %v", len(names), names)
			return false
		}

		// And replay the data.
		decoder, err := journal.SnapshotDecoder()
		if err != nil {
			fmt.Printf("error getting snapshot decoder: %v", err)
			return false
		}

		replayed := make([]dtype, 0, len(data))
		for {
			var val dtype
			err := decoder.Decode(&val)
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Printf("error decoding value in snapshot: %v", err)
				return false
			}
			replayed = append(replayed, val)
		}

		// And finally test that we have equality.
		for i, d := range data {
			if d != replayed[i] {
				fmt.Printf("expected\n\t%v\ngot\n\t%v on replay", data, replayed)
				return false
			}
		}

		select {
		case err := <-appendErrorChan:
			if err != nil {
				fmt.Printf("error appending to journal: %v", err)
				return false
			}
		default:
		}
		return true
	}

	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}
