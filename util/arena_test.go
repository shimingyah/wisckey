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

func TestArenaGrow(t *testing.T) {
	a := NewArena(128)
	n := a.Grow(64, 0)
	if n != 1 {
		t.Fatal("grow 64 bytes start pos error")
	}
	n = a.Grow(7, 8)
	if n != 65 {
		t.Fatal("grow 7 bytes with align 8 start pos error")
	}
	n = a.Grow(7, 16)
	if n != 96 {
		t.Fatal("grow 7 bytes with align 16 start pos error")
	}
}
