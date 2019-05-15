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

package util

import (
	"fmt"
	"sync/atomic"
)

// Allocator the interface of allocator
type Allocator interface {
	Grow(n, align uint32) uint32
	Acquire(offset, size uint32) []byte
	Allocate(n uint32) []byte
	AllocateAligned(n, align uint32) []byte
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

// Grow grows the buffer to guarantee space for n more bytes.
// If the free space isn't enough, it will panic.
func (a *Arena) Grow(n, align uint32) uint32 {
	// Pad the allocation with enough bytes to ensure alignment.
	l := uint32(n + align)
	total := atomic.AddUint32(&a.off, l)
	if total > uint32(len(a.buf)) {
		panic(fmt.Sprintf("Arena too small, alloc:%d align:%d newTotal:%d limit:%d", n, align, total, len(a.buf)))
	}

	m := total - l
	//  need to align
	if align != 0 {
		m = (total - l + uint32(align)) & ^uint32(align)
	}

	return m
}

// Acquire get the byte by given offset and size.
// it will painc when index out of slice.
func (a *Arena) Acquire(offset, size uint32) []byte {
	return a.buf[offset : offset+size]
}

// Allocate allocs n bytes of slice from the buffer.
// If the free space isn't enough, it will panic.
func (a *Arena) Allocate(n uint32) []byte {
	total := atomic.AddUint32(&a.off, n)
	if total > uint32(len(a.buf)) {
		panic(fmt.Sprintf("Arena too small, alloc:%d newTotal:%d limit:%d", n, total, len(a.buf)))
	}
	return a.buf[total-n : total]
}

// AllocateAligned align allocs n bytes of slice from the buffer.
// If the free space isn't enough, it will panic.
func (a *Arena) AllocateAligned(n, align uint32) []byte {
	// Pad the allocation with enough bytes to ensure alignment.
	l := uint32(n + align)
	total := atomic.AddUint32(&a.off, l)
	if total > uint32(len(a.buf)) {
		panic(fmt.Sprintf("Arena too small, alloc:%d align:%d newTotal:%d limit:%d", n, align, total, len(a.buf)))
	}

	// calc the aligned offset.
	m := (total - l + uint32(align)) & ^uint32(align)
	return a.buf[m : m+n]
}

// Size return used mem size.
func (a *Arena) Size() int64 {
	return int64(atomic.LoadUint32(&a.off))
}

// Reset reset arena.
func (a *Arena) Reset() {
	atomic.StoreUint32(&a.off, 0)
}
