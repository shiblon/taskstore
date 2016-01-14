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

package keyheap // import "entrogo.com/taskstore/keyheap"

import (
	"fmt"
)

type thing struct {
	id       int64
	priority int64
}

func (t *thing) Priority() int64 {
	return t.priority
}

func (t *thing) Key() int64 {
	return t.id
}

func (t *thing) String() string {
	return fmt.Sprintf("thing %v: priority=%v", t.id, t.priority)
}

func Example_new() {
	heap := New()
	fmt.Println(heap)

	// Output:
	// KeyHeap([])
}

func Example_newFromItems() {
	q := NewFromItems([]Item{
		&thing{1, 1000},
		&thing{2, 1004},
		&thing{3, 999},
		&thing{4, 1005},
		&thing{5, 1002},
		&thing{6, 1001},
		&thing{7, 1003},
	})

	fmt.Println(q)

	// Output:
	//
	// KeyHeap([
	//         {0:thing 3: priority=999}
	//         {1:thing 5: priority=1002}
	//         {2:thing 1: priority=1000}
	//         {3:thing 4: priority=1005}
	//         {4:thing 2: priority=1004}
	//         {5:thing 6: priority=1001}
	//         {6:thing 7: priority=1003}
	//      ])
}

func ExampleKeyHeap_Push() {
	q := NewFromItems([]Item{
		&thing{1, 1000},
		&thing{2, 1004},
		&thing{3, 999},
		&thing{4, 1005},
		&thing{5, 1002},
		&thing{6, 1001},
		&thing{7, 1003},
	})

	q.Push(&thing{8, 998})
	fmt.Println(q)

	// Output:
	//
	// KeyHeap([
	//         {0:thing 8: priority=998}
	//         {1:thing 3: priority=999}
	//         {2:thing 1: priority=1000}
	//         {3:thing 5: priority=1002}
	//         {4:thing 2: priority=1004}
	//         {5:thing 6: priority=1001}
	//         {6:thing 7: priority=1003}
	//         {7:thing 4: priority=1005}
	//      ])
}

func ExampleKeyHeap_Pop() {
	q := NewFromItems([]Item{
		&thing{1, 1000},
		&thing{2, 1004},
		&thing{3, 999},
		&thing{4, 1005},
		&thing{5, 1002},
		&thing{6, 1001},
		&thing{7, 1003},
	})

	// Pop the lowest priority item.
	thing := q.Pop()
	fmt.Println(thing)
	fmt.Println(q)

	// Output:
	//
	// thing 3: priority=999
	// KeyHeap([
	//         {0:thing 1: priority=1000}
	//         {1:thing 5: priority=1002}
	//         {2:thing 6: priority=1001}
	//         {3:thing 4: priority=1005}
	//         {4:thing 2: priority=1004}
	//         {5:thing 7: priority=1003}
	//      ])
}

func ExampleKeyHeap_PopAt() {
	q := NewFromItems([]Item{
		&thing{1, 1000},
		&thing{2, 1004},
		&thing{3, 999},
		&thing{4, 1005},
		&thing{5, 1002},
		&thing{6, 1001},
		&thing{7, 1003},
	})

	// Pop the lowest priority item.
	thing := q.PopAt(4)
	fmt.Println(thing)
	fmt.Println(q)

	// Output:
	//
	// thing 2: priority=1004
	// KeyHeap([
	//         {0:thing 3: priority=999}
	//         {1:thing 5: priority=1002}
	//         {2:thing 1: priority=1000}
	//         {3:thing 4: priority=1005}
	//         {4:thing 7: priority=1003}
	//         {5:thing 6: priority=1001}
	//      ])
}

func ExampleKeyHeap_Peek() {
	q := NewFromItems([]Item{
		&thing{1, 1000},
		&thing{2, 1004},
		&thing{3, 999},
		&thing{4, 1005},
		&thing{5, 1002},
		&thing{6, 1001},
		&thing{7, 1003},
	})

	fmt.Println(q.Peek())

	// Output:
	//
	// thing 3: priority=999
}

func ExampleKeyHeap_PeekAt() {
	q := NewFromItems([]Item{
		&thing{1, 1000},
		&thing{2, 1004},
		&thing{3, 999},
		&thing{4, 1005},
		&thing{5, 1002},
		&thing{6, 1001},
		&thing{7, 1003},
	})

	fmt.Println(q.PeekAt(3))

	// Output:
	//
	// thing 4: priority=1005
}

func ExampleKeyHeap_PeekByKey() {
	q := NewFromItems([]Item{
		&thing{1, 1000},
		&thing{2, 1004},
		&thing{3, 999},
		&thing{4, 1005},
		&thing{5, 1002},
		&thing{6, 1001},
		&thing{7, 1003},
	})

	fmt.Println(q.PeekByKey(5))

	// Output:
	//
	// thing 5: priority=1002
}

func ExampleKeyHeap_PopByKey() {
	q := NewFromItems([]Item{
		&thing{1, 1000},
		&thing{2, 1004},
		&thing{3, 999},
		&thing{4, 1005},
		&thing{5, 1002},
		&thing{6, 1001},
		&thing{7, 1003},
	})

	fmt.Println(q.PopByKey(2))
	fmt.Println(q)

	// Output:
	//
	// thing 2: priority=1004
	// KeyHeap([
	//         {0:thing 3: priority=999}
	//         {1:thing 5: priority=1002}
	//         {2:thing 1: priority=1000}
	//         {3:thing 4: priority=1005}
	//         {4:thing 7: priority=1003}
	//         {5:thing 6: priority=1001}
	//      ])
}

func ExampleKeyHeap_PopRandomAvailable_onlyOne() {
	q := NewFromItems([]Item{
		&thing{1, 1000},
		&thing{2, 1004},
		&thing{3, 999},
		&thing{4, 1005},
		&thing{5, 1002},
		&thing{6, 1001},
		&thing{7, 1003},
	})

	thing := q.PopRandomConstrained(999) // Only one matches.
	fmt.Println(thing)

	// Output:
	//
	// thing 3: priority=999
}

func ExampleKeyHeap_PopRandomAvailable_none() {
	q := NewFromItems([]Item{
		&thing{1, 1000},
		&thing{2, 1004},
		&thing{3, 999},
		&thing{4, 1005},
		&thing{5, 1002},
		&thing{6, 1001},
		&thing{7, 1003},
	})

	thing := q.PopRandomConstrained(998) // None match
	fmt.Println(thing)

	// Output:
	//
	// <nil>
}

func ExampleKeyHeap_PopRandomAvailable_random() {
	q := NewFromItems([]Item{
		&thing{1, 1000},
		&thing{2, 1004},
		&thing{3, 999},
		&thing{4, 1005},
		&thing{5, 1002},
		&thing{6, 1001},
		&thing{7, 1003},
	})

	t := q.PopRandomConstrained(1001)

	switch t.(*thing).id {
	case 1, 3, 6:
		fmt.Println("Yes")
	default:
		fmt.Println("No")
	}

	// Output:
	//
	// Yes
}
