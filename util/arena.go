// Copyright 2019 shimingyah. All rights reserved.
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

package util

import (
	"fmt"
	"sync/atomic"
)

// Allocator the interface of allocator
type Allocator interface {
	Allocate(n uint32) []byte
	AllocateAligned(n, align uint32) []byte
	AllocateSlot(n uint32) (begin, end uint32)
	AllocateSlotAligned(n, align uint32) (begin, end uint32)
	Acquire(offset, size uint32) []byte
	Size() int64
	Reset()
}

// Arena should be lock-free.
type Arena struct {
	off uint32
	buf []byte
}

// NewArena returns a new arena.
func NewArena(n int64) *Arena {
	// Don't store data at position 0 in order to reserve
	// offset=0 as a kind of nil pointer.
	a := &Arena{
		off: 1,
		buf: make([]byte, n),
	}
	return a
}

// AllocateSlot allocate n bytes return the pos begin and end.
// If the free space isn't enough, it will panic.
func (a *Arena) AllocateSlot(n uint32) (begin, end uint32) {
	total := atomic.AddUint32(&a.off, n)
	if total > uint32(len(a.buf)) {
		panic(fmt.Sprintf("Arena too small, alloc:%d newTotal:%d limit:%d", n, total, len(a.buf)))
	}

	m := total - n
	return m, m + n
}

// AllocateSlotAligned allocate aligned n bytes return the pos begin and end.
// If the free space isn't enough, it will panic.
func (a *Arena) AllocateSlotAligned(n, align uint32) (begin, end uint32) {
	// Pad the allocation with enough bytes to ensure alignment.
	l := uint32(n + align)
	total := atomic.AddUint32(&a.off, l)
	if total > uint32(len(a.buf)) {
		panic(fmt.Sprintf("Arena too small, alloc:%d align:%d newTotal:%d limit:%d", n, align, total, len(a.buf)))
	}

	m := (total - l + uint32(align)) & ^uint32(align)
	return m, m + n
}

// Acquire get the byte by given offset and size.
// it will painc when index out of slice.
func (a *Arena) Acquire(offset, size uint32) []byte {
	return a.buf[offset : offset+size]
}

// Allocate allocs n bytes of slice from the buffer.
// If the free space isn't enough, it will panic.
func (a *Arena) Allocate(n uint32) []byte {
	m, n := a.AllocateSlot(n)
	return a.buf[m:n]
}

// AllocateAligned align allocs n bytes of slice from the buffer.
// If the free space isn't enough, it will panic.
func (a *Arena) AllocateAligned(n, align uint32) []byte {
	m, n := a.AllocateSlotAligned(n, align)
	return a.buf[m:n]
}

// Size return used mem size.
func (a *Arena) Size() int64 {
	return int64(atomic.LoadUint32(&a.off))
}

// Reset reset arena.
func (a *Arena) Reset() {
	atomic.StoreUint32(&a.off, 0)
}
