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
	"testing"
)

func TestArenaAllocate(t *testing.T) {
	a := NewArena(128)
	if a.Size() != 1 {
		t.Fatal("init arena size isn't 1")
	}
	b := a.Allocate(64)
	if len(b) != 64 {
		t.Fatal("allocate 64 bytes length error")
	}
	if a.Size() != 64+1 {
		t.Fatal("allocate 64 bytes used error")
	}
}

func TestArenaAllocateAlign(t *testing.T) {
	a := NewArena(128)
	b := a.AllocateAligned(7, 8)
	if len(b) != 7 {
		t.Fatal("allocate 7 bytes length error")
	}
	if a.Size() != 7+8+1 {
		t.Fatal("allocate 7 bytes used error")
	}
	b = a.AllocateAligned(5, 16)
	if len(b) != 5 {
		t.Fatal("allocate 5 bytes length error")
	}
	if a.Size() != 5+16+7+8+1 {
		t.Fatal("allocate 5 bytes used error")
	}
}

func TestArenaAllocateSlot(t *testing.T) {
	a := NewArena(128)
	begin, end := a.AllocateSlot(64)
	if end-begin != 64 {
		t.Fatal("allocate slot 64 length error")
	}
	if begin != 1 || end != 64+1 {
		t.Fatal("allocate slot 64 range error")
	}

	begin, end = a.AllocateSlot(25)
	if end-begin != 25 {
		t.Fatal("allocate slot 25 length error")
	}
	if begin != 64+1 || end != 64+1+25 {
		t.Fatal("allocate slot 25 range error")
	}
}

func TestArenaAllocateSlotAlign(t *testing.T) {
	a := NewArena(128)
	begin, end := a.AllocateSlotAligned(5, 7)
	if end-begin != 5 {
		t.Fatal("allocate slot aligned 5 length error")
	}
	if begin != 8 {
		t.Fatal("allocate slot aligned 5 start error")
	}
	begin, end = a.AllocateSlotAligned(23, 7)
	if end-begin != 23 {
		t.Fatal("allocate slot aligned 23 length error")
	}
	if begin != 16 {
		t.Fatal("allocate slot aligned 23 start error")
	}
}
