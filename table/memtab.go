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

package table

import (
	"bytes"
	"encoding/binary"
)

// MemTable the interface of memtable
type MemTable interface {
	IncrRef()
	DecrRef()
	MemSize() int64
	Empty() bool
	Put(key []byte, value LSMValue)
	Get(key []byte) LSMValue
	NewIterator() *Iterator
	NewUniIterator(reversed bool) *UniIterator
}

type memTable struct {
	*Skiplist
}

// NewMemTable make a new empty mem table.
func NewMemTable(arenaSize int64, options ...Option) MemTable {
	return &memTable{Skiplist: NewSkiplist(arenaSize, options...)}
}

func (mt *memTable) Put(key []byte, value LSMValue) {
	mt.Skiplist.Put(key, nil)
}

func (mt *memTable) Get(key []byte) LSMValue {
	value := mt.Skiplist.Get(key)
	if value == nil {
		return LSMValue{}
	}
	//util.ParseTs(value)
}

// LSMValue represents the value info that can be associated with a key
// but also the internal Meta field.
type LSMValue struct {
	// Internal meta, it means how to handle this LSM value.
	Meta byte

	// User defined meta.
	UserMeta byte

	// Tne expires of value.
	ExpiresAt uint64

	// It will be value address or value self.
	Value []byte

	// This field is not serialized. Only for internal usage.
	Version uint64
}

// Encode expects a slice of length at least v.EncodedSize().
func (v *LSMValue) Encode(b []byte) {
	b[0] = v.Meta
	b[1] = v.UserMeta
	sz := binary.PutUvarint(b[2:], v.ExpiresAt)
	copy(b[2+sz:], v.Value)
}

// Decode uses the length of the slice to infer the length of the Value field.
func (v *LSMValue) Decode(b []byte) {
	v.Meta = b[0]
	v.UserMeta = b[1]
	var sz int
	v.ExpiresAt, sz = binary.Uvarint(b[2:])
	v.Value = b[2+sz:]
}

// EncodeTo should be kept in sync with the Encode function above. The reason
// this function exists is to avoid creating byte arrays per key-value pair in
// table/builder.go.
func (v *LSMValue) EncodeTo(buf *bytes.Buffer) {
	buf.WriteByte(v.Meta)
	buf.WriteByte(v.UserMeta)
	var enc [binary.MaxVarintLen64]byte
	sz := binary.PutUvarint(enc[:], v.ExpiresAt)
	buf.Write(enc[:sz])
	buf.Write(v.Value)
}
