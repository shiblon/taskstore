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

package journal // import "entrogo.com/taskstore/journal"

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath" // only use name manipulation, nothing that touches the file system.
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	ErrNotOpen = errors.New("journal is not open")

	// Logf is a function used to log warnings, etc. It can be overridden for
	// e.g., testing log output.
	Logf = func(fstr string, vals ...interface{}) {
		log.Printf(fstr, vals...)
	}
)

const (
	journalMaxRecords = 50000
	journalMaxAge     = time.Hour * 24

	// allow a ten-second clock correction before panicking. Yes, it's arbitrary.
	clockSkewLeeway = 10
)

type DiskLog struct {
	dir string
	fs  FS

	journalName      string
	journalFile      File
	journalEnc       *gob.Encoder
	journalBirth     time.Time
	journalRecords   int
	lastSnapshotTime time.Time
	isOpen           bool

	rot  chan chan error
	add  chan addRequest
	snap chan snapRequest
	quit chan chan error
}

type addRequest struct {
	rec  interface{}
	resp chan error
}

type snapRequest struct {
	elems    <-chan interface{}
	snapresp chan<- error
	resp     chan error
}

func OpenDiskLog(dir string) (*DiskLog, error) {
	// Default implementation just uses standard os module
	return OpenDiskLogInjectFS(dir, OSFS{})
}

func OpenDiskLogInjectFS(dir string, fs FS) (*DiskLog, error) {
	if info, err := fs.Stat(dir); err != nil {
		return nil, fmt.Errorf("Unable to stat %q: %v", dir, err)
	} else if !info.IsDir() {
		return nil, fmt.Errorf("Path %q is not a directory", dir)
	}

	d := &DiskLog{
		dir:  dir,
		add:  make(chan addRequest, 1),
		rot:  make(chan chan error, 1),
		snap: make(chan snapRequest, 1),
		quit: make(chan chan error, 1),
		fs:   fs,
	}

	// We *always* open a new log, even if there was one open when we last terminated.
	// This allows us to ignore any corrupt records at the end of the old one
	// without doing anything complicated to find out where they are, etc. Just
	// open a new log and have done with it. It's simpler and safer.
	d.openNewLog()

	go func() {
		for {
			select {
			case req := <-d.add:
				req.resp <- d.addRecord(req.rec)
			case resp := <-d.rot:
				resp <- d.rotateLog()
			case req := <-d.snap:
				req.resp <- d.snapshot(req.elems, req.snapresp)
			case resp := <-d.quit:
				err := d.freezeLog()
				d.isOpen = false
				resp <- err
				return
			}
		}
	}()

	return d, nil
}

// Close gracefully shuts the journal down, finalizing the current journal log.
func (d *DiskLog) Close() error {
	resp := make(chan error, 1)
	d.quit <- resp
	return <-resp
}

func (d *DiskLog) IsOpen() bool {
	return d.isOpen
}

// addRecord attempts to append the record to the end of the file, using gob encoding.
func (d *DiskLog) addRecord(rec interface{}) error {
	if !d.isOpen {
		return ErrNotOpen
	}
	if err := d.journalEnc.Encode(rec); err != nil {
		return err
	}
	if err := d.journalFile.Sync(); err != nil {
		return err
	}
	d.journalRecords++
	age := time.Now().Sub(d.journalBirth)
	if age >= journalMaxAge || d.journalRecords >= journalMaxRecords {
		if err := d.rotateLog(); err != nil {
			return err
		}
	}
	return nil
}

// TSFromName gets a timestamp from the file name (it's a prefix).
func TSFromName(name string) (int64, error) {
	name = filepath.Base(name)
	pos := strings.IndexRune(name, '.')
	if pos < 0 {
		return -1, fmt.Errorf("weird name, can't find ID: %q", name)
	}
	return strconv.ParseInt(name[:pos], 10, 64)
}

// snapshot attempts to get data elements from the caller and write them all to
// a snapshot file. It always triggers a log rotation, so that any other data
// that comes in (not part of the snapshot) is strictly newer.
func (d *DiskLog) snapshot(elems <-chan interface{}, resp chan<- error) error {
	if !d.isOpen {
		return ErrNotOpen
	}
	lastbirth := d.journalBirth
	if err := d.rotateLog(); err != nil {
		return err
	}
	// Once the rotation is complete, we try to create a file (still
	// synchronous) and then kick off an asynchronous snapshot process.
	snapname := filepath.Join(d.dir, fmt.Sprintf("%d.%d.snapshot.working", lastbirth.Unix(), os.Getpid()))
	donename := strings.TrimSuffix(snapname, ".working")
	file, err := d.fs.Create(snapname)
	if err != nil {
		return err
	}
	encoder := gob.NewEncoder(file)
	go func() {
		// make sure we consume all of them to play nicely with the producer.
		defer func() {
			num := 0
			for _ = range elems {
				num++
			}
			if num > 0 {
				Logf("consumed but did not snapshot %d element(s)", num)
			}
		}()

		for elem := range elems {
			if err := encoder.Encode(elem); err != nil {
				errtxt := fmt.Sprintf("snapshot failed to encode element %#v: %v", elem, err)
				if err := file.Close(); err != nil {
					errtxt += fmt.Sprintf(" -- also failed to close file %q: %v", file.Name(), err)
				}
				resp <- errors.New(errtxt)
				return
			}
		}
		if err := file.Close(); err != nil {
			resp <- fmt.Errorf("failed to close snapshot file %q: %v", snapname, err)
			return
		}

		// Now we indicate that the file is finished by renaming it.
		if err := d.fs.Rename(snapname, donename); err != nil {
			resp <- fmt.Errorf("snapshot incomplete, failed to rename %q to %q: %v", snapname, donename, err)
			return
		}

		// Finally, we delete all of the journal files that participated up to this point.
		doneglob := filepath.Join(d.dir, "*.*.log")
		workglob := filepath.Join(d.dir, "*.*.log.working")
		donenames, err := d.fs.FindMatching(doneglob)
		if err != nil {
			Logf("finished name glob %q failed: %v", doneglob, err)
		}
		worknames, err := d.fs.FindMatching(workglob)
		if err != nil {
			Logf("working name glob %q failed: %v", workglob, err)
		}
		names := make([]string, 0, len(donenames)+len(worknames))
		names = append(names, donenames...)
		names = append(names, worknames...)

		// Mark all previous journals, finished or otherwise, as obsolete.
		maxts := lastbirth.Unix()
		for _, name := range names {
			ts, err := TSFromName(name)
			if err != nil {
				Logf("skipping unknown name format %q: %v", name, err)
				continue
			}
			if ts > maxts {
				continue
			}

			// Finally, rename this log file to an obsolete name so that it can be cleaned up later.
			var obsname string
			if strings.HasSuffix(name, ".working") {
				obsname = fmt.Sprintf("%s.defunct", strings.TrimSuffix(name, ".working"))
			} else {
				obsname = fmt.Sprintf("%s.obsolete", name)
			}
			if err := d.fs.Rename(name, obsname); err != nil {
				Logf("failed to rename %q to %q: %v\n", name, obsname, err)
				continue
			}
		}
		resp <- nil
	}()
	return nil
}

// freezeLog closes the file for the current journal, nils out the appropriate
// members, and removes the ".working" suffix from the file name.
func (d *DiskLog) freezeLog() error {
	if !d.isOpen {
		return ErrNotOpen
	}
	jf := d.journalFile
	d.journalEnc = nil
	d.journalFile = nil

	if err := jf.Close(); err != nil {
		return fmt.Errorf("failed to close log: %v", err)
	}

	if !strings.HasSuffix(d.journalName, ".working") {
		return fmt.Errorf("trying to freeze an already-frozen log: %s", d.journalName)
	}
	if err := d.fs.Rename(d.journalName, strings.TrimSuffix(d.journalName, ".working")); err != nil {
		return fmt.Errorf("failed to freeze %s by rename: %v", d.journalName, err)
	}

	return nil
}

// openNewLog creates a new log file and sets it as the current log. It does
// not check whether another one is already open, it just abandons it without
// closing it.
func (d *DiskLog) openNewLog() error {
	// Make sure we don't rotate into the past. That will mess everything up.
	var name string
	oldbirth := d.journalBirth
	birth := time.Now()
	if birth.Unix() < oldbirth.Unix()-clockSkewLeeway {
		panic(fmt.Sprintf(
			"latest log created at timestamp %d, which appears to be in the future; current time is %d\n"+
				"either the clock got changed, or too many rotations have happened in a short period of time",
			oldbirth.Unix(), birth.Unix()))
	} else if birth.Unix() <= oldbirth.Unix() {
		birth = oldbirth.Add(1 * time.Second)
	}
	name = filepath.Join(d.dir, fmt.Sprintf("%d.%d.log.working", birth.Unix(), os.Getpid()))

	f, err := d.fs.Create(name)
	if err != nil {
		return err
	}
	d.journalBirth = birth
	d.journalName = name
	d.journalRecords = 0
	d.journalFile = f
	d.journalEnc = gob.NewEncoder(f)
	d.isOpen = true
	return nil
}

// rotateLog closes and freezes the current log and opens a new one.
func (d *DiskLog) rotateLog() error {
	if !d.isOpen {
		return ErrNotOpen
	}
	if err := d.freezeLog(); err != nil {
		return err
	}
	if err := d.openNewLog(); err != nil {
		return err
	}
	return nil
}

// Dir returns the file system directory for this journal.
func (d *DiskLog) Dir() string {
	return d.dir
}

// Return the current journal name.
func (d *DiskLog) JournalName() string {
	return d.journalName
}

// Append adds a record to the end of the journal.
func (d *DiskLog) Append(rec interface{}) error {
	resp := make(chan error, 1)
	d.add <- addRequest{
		rec,
		resp,
	}
	return <-resp
}

// StartSnapshot triggers an immediate rotation, then consumes all of the
// elements on the channel and serializing them to a snapshot file with the
// same ID as the recently-closed log.
func (d *DiskLog) StartSnapshot(elems <-chan interface{}, snapresp chan<- error) error {
	resp := make(chan error, 1)
	d.snap <- snapRequest{
		elems,
		snapresp,
		resp,
	}
	return <-resp
}

// Rotate closes the current log file and opens a new one.
func (d *DiskLog) Rotate() error {
	resp := make(chan error, 1)
	d.rot <- resp
	return <-resp
}

// latestFrozenSnapshot attempts to find the most recent snapshot on which to base journal replay.
func (d *DiskLog) latestFrozenSnapshot() (int64, string, error) {
	glob := filepath.Join(d.dir, fmt.Sprintf("*.*.snapshot"))
	names, err := d.fs.FindMatching(glob)
	if err != nil {
		return -1, "", err
	}
	if len(names) == 0 {
		return -1, "", io.EOF
	}
	bestts := int64(-1)
	bestname := ""
	for _, name := range names {
		ts, err := TSFromName(name)
		if err != nil {
			Logf("can't find id in in %q: %v", name, err)
			continue
		}
		if ts > bestts {
			bestts = ts
			bestname = name
		}
	}
	if bestts < 0 {
		return -1, "", io.EOF
	}
	return bestts, bestname, nil
}

// SnapshotDecoder returns a decoder whose Decode function can be called to get
// the next item from the most recent frozen snapshot.
func (d *DiskLog) SnapshotDecoder() (Decoder, error) {
	_, snapname, err := d.latestFrozenSnapshot()
	if err != nil && err != io.EOF {
		return nil, err
	}
	// Default empty implementation for the case where there just isn't a file.
	if err == io.EOF {
		return EmptyDecoder{}, nil
	}

	// Found it - try to open it for reading.
	file, err := d.fs.Open(snapname)
	if err != nil {
		return nil, err
	}
	return gob.NewDecoder(file), nil
}

// journalNames implements a Sorter interface, sorting based on timestamps.
type journalNames []string

func (n journalNames) Less(i, j int) bool {
	bi, _ := TSFromName(n[i])
	bj, _ := TSFromName(n[j])
	return bi < bj
}

func (n journalNames) Swap(i, j int) {
	n[i], n[j] = n[j], n[i]
}

func (n journalNames) Len() int {
	return len(n)
}

// gobMultiDecoder decodes gob entries from multiple readers in series. An
// io.MultiReader is not suitable here because each journal file can have a
// single corrupt entry at the end, so we have to gracefully handle
// ErrUnexpectedEOF in the logical *middle* of the whole journal stream.
type gobMultiDecoder struct {
	fs FS

	filenames []string
	cur       int

	decoder *gob.Decoder
}

func newGobMultiDecoder(fs FS, filenames ...string) (*gobMultiDecoder, error) {
	if len(filenames) == 0 {
		return nil, fmt.Errorf("gob multidecoder needs at least one file")
	}
	return &gobMultiDecoder{
		cur:       0,
		fs:        fs,
		filenames: filenames,
		decoder:   nil,
	}, nil
}

// newGobDecoder loads the file into memory and creates a gob.Decoder from it.
// As we are dealing with log files, and they really should not have any reason
// to get individually huge, it is safer and simpler to just load the file into
// RAM and the quickly close it instead of relying on the caller to consume all
// of the records quickly.
func (d *gobMultiDecoder) newGobDecoder(fname string) (*gob.Decoder, error) {
	file, err := d.fs.Open(fname)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := file.Close(); err != nil {
			panic(fmt.Sprintf("error closing file opened only for reading. should never happen: %v", err))
		}
	}()
	var buffer bytes.Buffer
	if _, err := buffer.ReadFrom(file); err != nil {
		return nil, err
	}
	return gob.NewDecoder(&buffer), nil
}

// Decode runs the decode function on each file in turn, skipping records that
// produce an ErrUnexpectedEOF. When we checksum records, it will also stop on
// those and verify that they are each the last such in their respective files.
func (d *gobMultiDecoder) Decode(val interface{}) error {
	// This only happens on the first call.
	if d.decoder == nil && d.cur == 0 {
		name := d.filenames[d.cur]
		decoder, err := d.newGobDecoder(name)
		if err != nil {
			Logf("failed to create decoder for file %q: %v", name, err)
			return err
		}
		d.decoder = decoder
	}
	err := d.decoder.Decode(val)
	for err == io.EOF || err == io.ErrUnexpectedEOF {
		if err == io.ErrUnexpectedEOF {
			Logf("journal file %q has an unexpected EOF", d.filenames[d.cur])
			// Try to read one more time, ensure we get an actual EOF.
			v := struct{}{}
			err := d.decoder.Decode(&v)
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				// OK - the next record really *wasn't* supposed to be the
				// last. Only the last record is allowed to be a partial write
				// or otherwise corrupt, so this is a real problem.
				return io.ErrUnexpectedEOF
			}
		}
		if d.cur++; d.cur >= len(d.filenames) {
			return io.EOF // really and truly finished, now.
		}
		name := d.filenames[d.cur]
		d.decoder, err = d.newGobDecoder(d.filenames[d.cur])
		if err != nil {
			Logf("failed to create decoder for file %q: %v", name, err)
			return err
		}
		err = d.decoder.Decode(val)
	}
	return err
}

// JournalDecoder returns a Decoder whose Decode function can be called to get
// the next item from the journals that are newer than the most recent
// snapshot.
func (d *DiskLog) JournalDecoder() (Decoder, error) {
	doneglob := filepath.Join(d.dir, fmt.Sprintf("*.*.log"))
	workglob := filepath.Join(d.dir, fmt.Sprintf("*.*.log.working"))
	donenames, err := d.fs.FindMatching(doneglob)
	switch {
	case err == io.EOF:
		return EmptyDecoder{}, nil
	case err != nil:
		return nil, err
	}

	worknames, err := d.fs.FindMatching(workglob)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(donenames)+len(worknames))
	names = append(names, donenames...)
	names = append(names, worknames...)

	sort.Sort(journalNames(names))

	snapbirth, _, err := d.latestFrozenSnapshot()
	if err != nil && err != io.EOF {
		return nil, err
	}
	for i, name := range names {
		ts, err := TSFromName(name)
		if err != nil {
			return nil, fmt.Errorf("failed to get timestamp from name %q: %v", name, err)
		}
		if ts > snapbirth {
			// Found the first journal file later than the snapshot. Return the decoder.
			return newGobMultiDecoder(d.fs, names[i:]...)
		}
	}
	// No journals found later than the snapshot.
	return EmptyDecoder{}, nil
}
