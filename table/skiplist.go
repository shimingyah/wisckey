// Copyright 2019 shimingyah. All rights reserved.
//
// Copyright 2017 Dgraph Labs, Inc. and Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// ee the License for the specific language governing permissions and
// limitations under the License.

/*
Adapted from RocksDB inline skiplist.

Key differences:
- No optimization for sequential inserts (no "prev").
- No custom comparator.
- Support overwrites. This requires care when we see the same key when inserting.
  For RocksDB or LevelDB, overwrites are implemented as a newer sequence number in the key, so
	there is no need for values. We don't intend to support versioning. In-place updates of values
	would be more efficient.
- We discard all non-concurrent code.
- We do not support Splices. This simplifies the code a lot.
- No AllocateNode or other pointer arithmetic.
- We combine the findLessThan, findGreaterOrEqual, etc into one function.
*/

package table

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"sync/atomic"
	"unsafe"

	"github.com/shimingyah/wisckey/util"
)

const (
	maxHeight      = 20
	heightIncrease = math.MaxUint32 / 3
)

const (
	// MaxNodeSize is the memory footprint of a node of maximum height.
	MaxNodeSize = int(unsafe.Sizeof(node{}))

	// Always align nodes on 64-bit boundaries, even on 32-bit architectures,
	// so that the node.value field is 64-bit aligned. This is necessary because
	// node.getValueOffset uses atomic.LoadUint64, which expects its input
	// pointer to be 64-bit aligned.
	nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1
)

// Skiplist maps keys to values (in memory)
type Skiplist struct {
	height        int32 // Current height. 1 <= height <= kMaxHeight. CAS.
	head          *node
	ref           int32
	allocator     util.Allocator
	KeyComparator func(key1, key2 []byte) int
	KeyEqualizer  func(key1, key2 []byte) bool
}

type node struct {
	// Multiple parts of the value are encoded as a single uint64 so that it
	// can be atomically loaded and stored:
	// value offset: uint32 (bits 0-31)
	// value size  : uint16 (bits 32-47)
	value uint64

	// A byte slice is 24 bytes. We are trying to save space here.
	keyOffset uint32 // Immutable. No need to lock to access key.
	keySize   uint16 // Immutable. No need to lock to access key.

	// Height of the tower.
	height uint16

	// Most nodes do not need to use the full height of the tower, since the
	// probability of each successive level decreases exponentially. Because
	// these elements are never accessed, they do not need to be allocated.
	// Therefore, when a node is allocated in the arena, its memory footprint
	// is deliberately truncated to not include unneeded tower elements.
	//
	// All accesses to elements should use CAS operations, with no need to lock.
	tower [maxHeight]uint32
}

// Option used to init user defined params
type Option func(s *Skiplist)

// KeyComparator user defined key comparator
func KeyComparator(keyComparator func(key1, key2 []byte) int) Option {
	return func(s *Skiplist) {
		s.KeyComparator = keyComparator
	}
}

// KeyEqualizer user defined key euqalizer used to get.
func KeyEqualizer(keyEqualizer func(key1, key2 []byte) bool) Option {
	return func(s *Skiplist) {
		s.KeyEqualizer = keyEqualizer
	}
}

func defaultKeyComparator(key1, key2 []byte) int {
	return bytes.Compare(key1, key2)
}

func defaultKeyEqualizer(key1, key2 []byte) bool {
	return bytes.Equal(key1, key2)
}

// NewSkiplist makes a new empty skiplist, with a given arena size
func NewSkiplist(arenaSize int64, options ...Option) *Skiplist {
	allocator := util.NewArena(arenaSize)
	head := newNode(allocator, nil, nil, maxHeight)
	s := &Skiplist{
		height:        1,
		head:          head,
		allocator:     allocator,
		ref:           1,
		KeyComparator: defaultKeyComparator,
		KeyEqualizer:  defaultKeyEqualizer,
	}

	for _, option := range options {
		option(s)
	}

	return s
}

// IncrRef increases the refcount
func (s *Skiplist) IncrRef() {
	atomic.AddInt32(&s.ref, 1)
}

// DecrRef decrements the refcount, deallocating the Skiplist when done using it
func (s *Skiplist) DecrRef() {
	newRef := atomic.AddInt32(&s.ref, -1)
	if newRef > 0 {
		return
	}

	if s.allocator != nil {
		s.allocator.Reset()
	}

	// Indicate we are closed. Good for testing.  Also, lets GC reclaim memory. Race condition
	// here would suggest we are accessing skiplist when we are supposed to have no reference!
	s.allocator = nil
}

// MemSize returns the size of the Skiplist in terms of
// how much memory is used within its internal allocator.
func (s *Skiplist) MemSize() int64 { return s.allocator.Size() }

// NewIterator returns a skiplist iterator.  You have to Close() the iterator.
func (s *Skiplist) NewIterator() *Iterator {
	s.IncrRef()
	return &Iterator{list: s}
}

// Empty returns if the Skiplist is empty.
func (s *Skiplist) Empty() bool {
	return s.findLast() == nil
}

// Get gets the value associated with the key. It returns a valid value if it finds equal or earlier
// version of the same key.
func (s *Skiplist) Get(key []byte) []byte {
	n, _ := s.findNear(key, false, true) // findGreaterOrEqual.
	if n == nil {
		return nil
	}

	nextKey := s.allocator.Acquire(n.keyOffset, uint32(n.keySize))
	if !s.KeyEqualizer(key, nextKey) {
		return nil
	}

	valOffset, valSize := n.getValueOffset()
	value := s.allocator.Acquire(uint32(valOffset), uint32(valSize))

	return value
}

// Put inserts the key-value pair.
func (s *Skiplist) Put(key, value []byte) {
	// Since we allow overwrite, we may not need to create a new node. We might not even need to
	// increase the height. Let's defer these actions.
	listHeight := s.getHeight()
	var prev [maxHeight + 1]*node
	var next [maxHeight + 1]*node
	prev[listHeight] = s.head
	next[listHeight] = nil
	for i := int(listHeight) - 1; i >= 0; i-- {
		// Use higher level to speed up for current level.
		prev[i], next[i] = s.findSpliceForLevel(key, prev[i+1], i)
		if prev[i] == next[i] {
			prev[i].setValue(s.allocator, value)
			return
		}
	}

	// We do need to create a new node.
	height := randomHeight()
	x := newNode(s.allocator, key, value, height)

	// Try to increase s.height via CAS.
	listHeight = s.getHeight()
	for height > int(listHeight) {
		if atomic.CompareAndSwapInt32(&s.height, listHeight, int32(height)) {
			// Successfully increased skiplist.height.
			break
		}
		listHeight = s.getHeight()
	}

	// We always insert from the base level and up. After you add a node in base level, we cannot
	// create a node in the level above because it would have discovered the node in the base level.
	for i := 0; i < height; i++ {
		for {
			if prev[i] == nil {
				util.AssertTrue(i > 1) // This cannot happen in base level.
				// We haven't computed prev, next for this level because height exceeds old listHeight.
				// For these levels, we expect the lists to be sparse, so we can just search from head.
				prev[i], next[i] = s.findSpliceForLevel(key, s.head, i)
				// Someone adds the exact same key before we are able to do so. This can only happen on
				// the base level. But we know we are not on the base level.
				util.AssertTrue(prev[i] != next[i])
			}
			nextOffset := getNodeOffset(s.allocator, next[i])
			x.tower[i] = nextOffset
			if prev[i].casNextOffset(i, nextOffset, getNodeOffset(s.allocator, x)) {
				// Managed to insert x between prev[i] and next[i]. Go to the next level.
				break
			}
			// CAS failed. We need to recompute prev and next.
			// It is unlikely to be helpful to try to use a different level as we redo the search,
			// because it is unlikely that lots of nodes are inserted between prev[i] and next[i].
			prev[i], next[i] = s.findSpliceForLevel(key, prev[i], i)
			if prev[i] == next[i] {
				util.AssertTruef(i == 0, "Equality can happen only on base level: %d", i)
				prev[i].setValue(s.allocator, value)
				return
			}
		}
	}
}

func (s *Skiplist) valid() bool { return s.allocator != nil }

func (s *Skiplist) getHeight() int32 {
	return atomic.LoadInt32(&s.height)
}

func (s *Skiplist) getNext(nd *node, height int) *node {
	return getNode(s.allocator, nd.getNextOffset(height))
}

// findNear finds the node near to key.
// If less=true, it finds rightmost node such that node.key < key (if allowEqual=false) or
// node.key <= key (if allowEqual=true).
// If less=false, it finds leftmost node such that node.key > key (if allowEqual=false) or
// node.key >= key (if allowEqual=true).
// Returns the node found. The bool returned is true if the node has key equal to given key.
func (s *Skiplist) findNear(key []byte, less bool, allowEqual bool) (*node, bool) {
	x := s.head
	level := int(s.getHeight() - 1)
	for {
		// Assume x.key < key.
		next := s.getNext(x, level)
		if next == nil {
			// x.key < key < END OF LIST
			if level > 0 {
				// Can descend further to iterate closer to the end.
				level--
				continue
			}
			// Level=0. Cannot descend further. Let's return something that makes sense.
			if !less {
				return nil, false
			}
			// Try to return x. Make sure it is not a head node.
			if x == s.head {
				return nil, false
			}
			return x, false
		}

		nextKey := next.key(s.allocator)
		cmp := s.KeyComparator(key, nextKey)
		if cmp > 0 {
			// x.key < next.key < key. We can continue to move right.
			x = next
			continue
		}
		if cmp == 0 {
			// x.key < key == next.key.
			if allowEqual {
				return next, true
			}
			if !less {
				// We want >, so go to base level to grab the next bigger note.
				return s.getNext(next, 0), false
			}
			// We want <. If not base level, we should go closer in the next level.
			if level > 0 {
				level--
				continue
			}
			// On base level. Return x.
			if x == s.head {
				return nil, false
			}
			return x, false
		}
		// cmp < 0. In other words, x.key < key < next.
		if level > 0 {
			level--
			continue
		}
		// At base level. Need to return something.
		if !less {
			return next, false
		}
		// Try to return x. Make sure it is not a head node.
		if x == s.head {
			return nil, false
		}
		return x, false
	}
}

// findLast returns the last element. If head (empty list), we return nil. All the find functions
// will NEVER return the head nodes.
func (s *Skiplist) findLast() *node {
	n := s.head
	level := int(s.getHeight()) - 1
	for {
		next := s.getNext(n, level)
		if next != nil {
			n = next
			continue
		}
		if level == 0 {
			if n == s.head {
				return nil
			}
			return n
		}
		level--
	}
}

// findSpliceForLevel returns (outBefore, outAfter) with outBefore.key <= key <= outAfter.key.
// The input "before" tells us where to start looking.
// If we found a node with the same key, then we return outBefore = outAfter.
// Otherwise, outBefore.key < key < outAfter.key.
func (s *Skiplist) findSpliceForLevel(key []byte, before *node, level int) (*node, *node) {
	for {
		// Assume before.key < key.
		next := s.getNext(before, level)
		if next == nil {
			return before, next
		}
		nextKey := next.key(s.allocator)
		cmp := s.KeyComparator(key, nextKey)
		if cmp == 0 {
			// Equality case.
			return next, next
		}
		if cmp < 0 {
			// before.key < key < next.key. We are done for this level.
			return before, next
		}
		before = next // Keep moving right on this level.
	}
}

func randomHeight() int {
	h := 1
	for h < maxHeight && rand.Uint32() <= heightIncrease {
		h++
	}
	return h
}

// newNode the base level is already allocated in the node struct.
func newNode(allocator util.Allocator, key, value []byte, height int) *node {
	offset := putNode(allocator, height)
	node := getNode(allocator, offset)
	node.keyOffset = putSlice(allocator, key)
	node.keySize = uint16(len(key))
	node.height = uint16(height)
	node.value = encodeValue(putSlice(allocator, value), uint16(len(value)))
	return node
}

func (n *node) getValueOffset() (uint32, uint16) {
	value := atomic.LoadUint64(&n.value)
	return decodeValue(value)
}

func (n *node) key(allocator util.Allocator) []byte {
	return allocator.Acquire(n.keyOffset, uint32(n.keySize))
}

func (n *node) setValue(allocator util.Allocator, v []byte) {
	valOffset := putSlice(allocator, v)
	value := encodeValue(valOffset, uint16(len(v)))
	atomic.StoreUint64(&n.value, value)
}

func (n *node) getNextOffset(h int) uint32 {
	return atomic.LoadUint32(&n.tower[h])
}

func (n *node) casNextOffset(h int, old, val uint32) bool {
	return atomic.CompareAndSwapUint32(&n.tower[h], old, val)
}

// putNode compute the amount of the tower that will never be used,
// since the height is less than maxHeight.
func putNode(allocator util.Allocator, height int) uint32 {
	unusedSize := (maxHeight - height) * int(unsafe.Sizeof(uint32(0)))
	begin, _ := allocator.AllocateSlotAligned(uint32(MaxNodeSize-unusedSize), uint32(nodeAlign))
	return begin
}

// getNode returns a pointer to the node located at offset. If the offset is
// zero, then the nil node pointer is returned.
func getNode(allocator util.Allocator, offset uint32) *node {
	if offset == 0 {
		return nil
	}
	buf := allocator.Acquire(offset, offset+1)
	return (*node)(unsafe.Pointer(&buf[0]))
}

// getNodeOffset returns the offset of node in the arena. If the node pointer is
// nil, then the zero offset is returned.
func getNodeOffset(allocator util.Allocator, nd *node) uint32 {
	if nd == nil {
		return 0
	}

	buf := allocator.Acquire(0, 1)
	return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&buf[0])))
}

func putSlice(allocator util.Allocator, v []byte) uint32 {
	begin, end := allocator.AllocateSlot(uint32(len(v)))
	n := copy(allocator.Acquire(begin, end-begin), v)
	if n != len(v) {
		panic(fmt.Sprintf("allocate copy error, begin:%d end:%d key:%v", begin, end, v))
	}
	return begin
}

func encodeValue(valOffset uint32, valSize uint16) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}

func decodeValue(value uint64) (valOffset uint32, valSize uint16) {
	valOffset = uint32(value)
	valSize = uint16(value >> 32)
	return
}

// Iterator is an iterator over skiplist object. For new objects, you just
// need to initialize Iterator.list.
type Iterator struct {
	list *Skiplist
	n    *node
}

// Close frees the resources held by the iterator
func (s *Iterator) Close() error {
	s.list.DecrRef()
	return nil
}

// Valid returns true iff the iterator is positioned at a valid node.
func (s *Iterator) Valid() bool { return s.n != nil }

// Key returns the key at the current position.
func (s *Iterator) Key() []byte {
	return s.list.allocator.Acquire(s.n.keyOffset, uint32(s.n.keySize))
}

// Value returns value.
func (s *Iterator) Value() []byte {
	valOffset, valSize := s.n.getValueOffset()
	return s.list.allocator.Acquire(uint32(valOffset), uint32(valSize))
}

// Next advances to the next position.
func (s *Iterator) Next() {
	util.AssertTrue(s.Valid())
	s.n = s.list.getNext(s.n, 0)
}

// Prev advances to the previous position.
func (s *Iterator) Prev() {
	util.AssertTrue(s.Valid())
	s.n, _ = s.list.findNear(s.Key(), true, false) // find <. No equality allowed.
}

// Seek advances to the first entry with a key >= target.
func (s *Iterator) Seek(key []byte) {
	s.n, _ = s.list.findNear(key, false, true) // find >=.
}

// SeekForPrev finds an entry with key <= target.
func (s *Iterator) SeekForPrev(key []byte) {
	s.n, _ = s.list.findNear(key, true, true) // find <=.
}

// SeekToFirst seeks position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *Iterator) SeekToFirst() {
	s.n = s.list.getNext(s.list.head, 0)
}

// SeekToLast seeks position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *Iterator) SeekToLast() {
	s.n = s.list.findLast()
}

// UniIterator is a unidirectional memtable iterator. It is a thin wrapper around
// Iterator. We like to keep Iterator as before, because it is more powerful and
// we might support bidirectional iterators in the future.
type UniIterator struct {
	iter     *Iterator
	reversed bool
}

// NewUniIterator returns a UniIterator.
func (s *Skiplist) NewUniIterator(reversed bool) *UniIterator {
	return &UniIterator{
		iter:     s.NewIterator(),
		reversed: reversed,
	}
}

// Next implements y.Interface
func (s *UniIterator) Next() {
	if !s.reversed {
		s.iter.Next()
	} else {
		s.iter.Prev()
	}
}

// Rewind implements y.Interface
func (s *UniIterator) Rewind() {
	if !s.reversed {
		s.iter.SeekToFirst()
	} else {
		s.iter.SeekToLast()
	}
}

// Seek implements y.Interface
func (s *UniIterator) Seek(key []byte) {
	if !s.reversed {
		s.iter.Seek(key)
	} else {
		s.iter.SeekForPrev(key)
	}
}

// Key implements y.Interface
func (s *UniIterator) Key() []byte { return s.iter.Key() }

// Value implements y.Interface
func (s *UniIterator) Value() []byte { return s.iter.Value() }

// Valid implements y.Interface
func (s *UniIterator) Valid() bool { return s.iter.Valid() }

// Close implements y.Interface (and frees up the iter's resources)
func (s *UniIterator) Close() error { return s.iter.Close() }
