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

// Package journal is an implementation and interface specification for an
// append-only journal with rotations. It contains a few simple implementations,
// as well.
package journal

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
)

var (
	ErrCantClose = errors.New("cannot close this type of journal")
)

type Interface interface {
	// Append appends a serialized version of the interface to the
	// current journal shard.
	Append(interface{}) error

	// StartSnapshot is given a data channel from which it is expected to
	// consume all values until closed. If it terminates early, it sends a
	// non-nil error back. When complete with no errors, the snapshot has been
	// successfully processed. Whether the current shard is full or not, this
	// function immediately trigger a shard rotation so that subsequent calls
	// to Append go to a new shard.
	StartSnapshot(records <-chan interface{}, resp chan<- error) error

	// SnapshotDecoder returns a decode function that can be called to decode
	// the next element in the most recent snapshot.
	SnapshotDecoder() (Decoder, error)

	// JournalDecoder returns a decode function that can be called to decode
	// the next element in the journal stream.
	JournalDecoder() (Decoder, error)

	// Close allows the journal to shut down (e.g., flush data) gracefully.
	Close() error

	// IsOpen indicates whether the journal is operational and has not been
	// closed.
	IsOpen() bool
}

type Decoder interface {
	// Decode attempts to fill the elements of the underlying value of its
	// argument with the next item.
	Decode(interface{}) error
}

// EmptyDecoder can be returned when there is nothing to decode, but it is safe
// to proceed.
type EmptyDecoder struct{}

// Decode with no elements - default behavior.
func (ed EmptyDecoder) Decode(interface{}) error {
	return io.EOF
}

type Bytes struct {
	enc  *gob.Encoder
	buff *bytes.Buffer
}

func NewBytes() *Bytes {
	j := &Bytes{
		buff: new(bytes.Buffer),
	}
	j.enc = gob.NewEncoder(j.buff)
	return j
}

func (j Bytes) Append(rec interface{}) error {
	return j.enc.Encode(rec)
}

func (j Bytes) Bytes() []byte {
	return j.buff.Bytes()
}

func (j Bytes) String() string {
	return j.buff.String()
}

func (j *Bytes) StartSnapshot(records <-chan interface{}, snapresp chan<- error) error {
	go func() {
		snapresp <- nil
	}()
	return nil
}

func (j *Bytes) SnapshotDecoder() (Decoder, error) {
	return EmptyDecoder{}, nil
}

func (j *Bytes) JournalDecoder() (Decoder, error) {
	return gob.NewDecoder(j.buff), nil
}

func (j *Bytes) Close() error {
	return ErrCantClose
}

func (j *Bytes) IsOpen() bool {
	return true
}

type Count int64

func NewCount() *Count {
	return new(Count)
}

func (j Count) ShardFinished() bool {
	return false
}

func (j *Count) Append(_ interface{}) error {
	*j++
	return nil
}

func (j Count) String() string {
	return fmt.Sprintf("records written = %d", j)
}

func (j Count) StartSnapshot(records <-chan interface{}, snapresp chan<- error) error {
	go func() {
		num := 0
		for _ = range records {
			num++
		}
		fmt.Printf("snapshotted %d records", num)
		snapresp <- nil
	}()
	return nil
}

func (j Count) SnapshotDecoder() (Decoder, error) {
	return EmptyDecoder{}, nil
}

func (j Count) JournalDecoder() (Decoder, error) {
	return EmptyDecoder{}, nil
}

func (j Count) Close() error {
	return ErrCantClose
}

func (j Count) IsOpen() bool {
	return true
}
