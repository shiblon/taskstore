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

/*
Package keyheap implements a library for a simple heap that allows peeking and
popping from the middle based on a Key() in the stored interface.
*/
package keyheap // import "entrogo.com/taskstore/keyheap"

import (
	"container/heap"
	"fmt"
	"math"
	"math/rand"
	"strings"
)

type Item interface {
	// Priority is used to determine which element (lowest priority) is at the
	// top of the heap.
	Priority() int64

	// Key is used to look up individual items inside of the heap.
	Key() int64
}

type KeyHeap struct {
	itemHeap heapImpl
	itemMap  heapMap
}

// New creates a new empty KeyHeap.
func New() *KeyHeap {
	return NewFromItems(nil)
}

// NewFromItems creates a KeyHeap from a slice of Item.
func NewFromItems(items []Item) *KeyHeap {
	q := &KeyHeap{
		itemHeap: make(heapImpl, len(items)),
		itemMap:  make(heapMap),
	}
	for i, item := range items {
		ti := &indexedItem{index: i, item: item}
		q.itemHeap[i] = ti
		q.itemMap[item.Key()] = ti
	}

	if q.Len() > 1 {
		heap.Init(&q.itemHeap)
	}
	return q
}

// String formats this heap into a string.
func (q *KeyHeap) String() string {
	hpieces := []string{"["}
	for _, v := range q.itemHeap {
		hpieces = append(hpieces, fmt.Sprintf("   %s", v))
	}
	if len(hpieces) == 1 {
		hpieces[0] += "]"
	} else {
		hpieces = append(hpieces, "]")
	}
	return fmt.Sprintf("KeyHeap(%v)", strings.Join(hpieces, "\n     "))
}

// Push adds an Item to the heap.
func (q *KeyHeap) Push(item Item) {
	ti := &indexedItem{item: item, index: -1}
	heap.Push(&q.itemHeap, ti)
	q.itemMap[item.Key()] = ti
}

// Pop removes the Item with the lowest Priority() from the KeyHeap.
func (q *KeyHeap) Pop() Item {
	ti := heap.Pop(&q.itemHeap).(*indexedItem)
	delete(q.itemMap, ti.item.Key())
	return ti.item
}

// PopAt removes an element from the specified index in the heap in O(log(n)) time.
func (q *KeyHeap) PopAt(idx int) Item {
	ti := heap.Remove(&q.itemHeap, idx).(*indexedItem)
	if ti == nil {
		return nil
	}
	delete(q.itemMap, ti.item.Key())
	return ti.item
}

// Len returns the size of the heap.
func (q *KeyHeap) Len() int {
	return len(q.itemHeap)
}

// Peek returns the top element in the heap (with the smallest Priority()), or nil if the heap is empty.
func (q *KeyHeap) Peek() Item {
	return q.PeekAt(0)
}

// PeekAt finds the item at index idx in the heap and returns it. Returns nil if idx is out of bounds.
func (q *KeyHeap) PeekAt(idx int) Item {
	if idx >= q.Len() {
		return nil
	}
	return q.itemHeap[idx].item
}

// PeekByKey finds the item with the given Key() and returns it, or nil if not found.
func (q *KeyHeap) PeekByKey(key int64) Item {
	ti := q.itemMap[key]
	if ti == nil {
		return nil
	}
	return ti.item
}

// PopByKey finds the item with the given Key() and returns it, removing it
// from the data structure.
func (q *KeyHeap) PopByKey(key int64) Item {
	ti := q.itemMap[key]
	if ti == nil {
		return nil
	}
	return q.PopAt(ti.index)
}

// PopRandomConstrained walks the heap randomly choosing a child until it either
// picks one or runs out (and picks the last one before the maxPriority). If
// maxPriority <= 0, then there is no constraint.
// Note that this greatly favors items near the top, because the probability of
// traversing the tree very far quickly gets vanishingly small. There are
// undoubtedly other interesting approaches to doing this.
func (q *KeyHeap) PopRandomConstrained(maxPriority int64) Item {
	// Start at the leftmost location (the lowest value), and randomly jump to
	// children so long as they are earlier than the maxPriority.
	idx := 0
	chosen := -1
	for idx < q.Len() && q.PeekAt(idx).Priority() <= maxPriority {
		left := idx*2 + 1
		right := left + 1
		choices := []int{idx}
		if left < q.Len() && q.PeekAt(left).Priority() <= maxPriority {
			choices = append(choices, left)
		}
		if right < q.Len() && q.PeekAt(right).Priority() <= maxPriority {
			choices = append(choices, right)
		}
		if len(choices) == 0 {
			break
		}
		choiceIndex := int(math.Floor(rand.Float64() * float64(len(choices))))
		if choiceIndex == 0 {
			chosen = choices[choiceIndex]
			break
		}
		// If we didn't choose the current node, redo the random draw with one of
		// the children as the new heap root.
		idx = choices[choiceIndex]
	}
	if chosen < 0 {
		return nil
	}
	return q.PopAt(chosen)
}

type indexedItem struct {
	index int
	item  Item
}

func (ti *indexedItem) String() string {
	return fmt.Sprintf("{%d:%v}", ti.index, ti.item)
}

type heapImpl []*indexedItem
type heapMap map[int64]*indexedItem

func (h heapImpl) Len() int {
	return len(h)
}

func (h heapImpl) Less(i, j int) bool {
	return h[i].item.Priority() < h[j].item.Priority()
}

func (h heapImpl) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *heapImpl) Push(x interface{}) {
	item := x.(*indexedItem)
	item.index = len(*h)
	*h = append(*h, item)
}

func (h *heapImpl) Pop() interface{} {
	n := len(*h)
	item := (*h)[n-1]
	item.index = -1
	*h = (*h)[:n-1]
	return item
}
