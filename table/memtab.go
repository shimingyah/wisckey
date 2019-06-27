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

package table

import (
	"bytes"
	"encoding/binary"

	"github.com/shimingyah/wisckey/util"
)

// MemTable the interface of memtable
type MemTable interface {
	IncrRef()
	DecrRef()
	MemSize() int64
	Empty() bool
	Put(key []byte, value LSMValue)
	Get(key []byte) LSMValue
	NewIterator() *util.Iterator
	NewUniIterator(reversed bool) *util.UniIterator
}

type memTable struct {
	*util.Skiplist
}

// NewMemTable make a new empty mem table.
func NewMemTable(arenaSize int64) MemTable {
	return &memTable{Skiplist: util.NewSkiplist(arenaSize,
		util.KeyComparator(MemTabKeyComparator), util.KeyEqualizer(MemTabKeyEqualizer))}
}

// MemTabKeyComparator checks the key without timestamp and checks the timestamp if keyNoTs
// is same.
// a<timestamp> would be sorted higher than aa<timestamp> if we use bytes.compare
// All keys should have timestamp.
func MemTabKeyComparator(key1, key2 []byte) int {
	util.AssertTrue(len(key1) > 8 && len(key2) > 8)
	if cmp := bytes.Compare(key1[:len(key1)-8], key2[:len(key2)-8]); cmp != 0 {
		return cmp
	}
	return bytes.Compare(key1[len(key1)-8:], key2[len(key2)-8:])
}

// MemTabKeyEqualizer checks for key equality ignoring the version timestamp suffix.
func MemTabKeyEqualizer(key1, key2 []byte) bool {
	if len(key1) != len(key2) {
		return false
	}
	return bytes.Equal(util.ParseKey(key1), util.ParseKey(key2))
}

// Put inserts the key-value pair.
// It will happen mem copy between memTab --> skiplist
// We can ignore the mem increase, because of the lsm tree data is small.
func (mt *memTable) Put(key []byte, value LSMValue) {
	buf := &bytes.Buffer{}
	value.EncodeTo(buf)
	mt.Skiplist.Put(key, buf.Bytes())
}

// Get gets the value associated with the key.
func (mt *memTable) Get(key []byte) LSMValue {
	nextkey, value := mt.Skiplist.GetWithKey(key)
	if value == nil {
		return LSMValue{}
	}

	lsmValue := &LSMValue{}
	lsmValue.Decode(value)
	lsmValue.Version = util.ParseTs(nextkey)
	return *lsmValue
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

// EncodedSize is the size of the ValueStruct when encoded
func (v *LSMValue) EncodedSize() uint16 {
	sz := len(v.Value) + 2 // meta, usermeta.
	if v.ExpiresAt == 0 {
		return uint16(sz + 1)
	}

	enc := sizeVarint(v.ExpiresAt)
	return uint16(sz + enc)
}

func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}