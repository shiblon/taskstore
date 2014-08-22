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
package keyheap

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
	randChan chan float64
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
		randChan: make(chan float64, 1),
	}
	for i, item := range items {
		ti := &indexedItem{index: i, item: item}
		q.itemHeap[i] = ti
		q.itemMap[item.Key()] = ti
	}
	// Provide thread-safe random values.
	go func() {
		for {
			q.randChan <- rand.Float64()
		}
	}()

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
	item := q.PeekAt(idx)
	if item == nil {
		return nil
	}
	// This uses basic heap operations to accomplish removal from the middle. A
	// couple of key things make this possible.
	// - A subslice still points to the underlying array, and has capacity extending to the end.
	// - Adding a smallest element to a prefix heap does not invalidate the rest of the heap.
	// - Pushing an element onto a heap puts it at the end and bubbles it up.
	// So, we take the heap prefix up to but not including idx and push the nil item.
	// This overwrites the element we want to remove with nil (adds to the end
	// of the prefix heap, overwriting underlying array storage, since capacity
	// is still there), and bubbles it to the very top (see Less below, it knows about nil).
	subheap := q.itemHeap[:idx]
	heap.Push(&subheap, &indexedItem{item: nil})
	if q.itemHeap[0].item != nil {
		panic("Bubbled nil item to top, but it didn't make it.")
	}
	// Then we remove the nil item at the top.
	heap.Pop(&q.itemHeap)
	delete(q.itemMap, item.Key())

	return item
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
		choices := make([]int, 1, 3)
		choices[0] = idx
		if left < q.Len() && q.PeekAt(left).Priority() <= maxPriority {
			choices = append(choices, left)
		}
		if right < q.Len() && q.PeekAt(right).Priority() <= maxPriority {
			choices = append(choices, right)
		}
		if len(choices) == 0 {
			break
		}
		choiceIndex := int(math.Floor(<-q.randChan * float64(len(choices))))
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
	if h[i].item == nil {
		return true
	} else if h[j].item == nil {
		return false
	}
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
