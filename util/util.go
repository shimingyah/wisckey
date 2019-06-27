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
	"encoding/binary"
	"math"

	"github.com/pkg/errors"
)

// ErrEOF indicates an end of file when trying to read from a memory mapped file
// and encountering the end of slice.
var ErrEOF = errors.New("End of mapped region")

// ParseTs parses the timestamp from the key bytes.
func ParseTs(key []byte) uint64 {
	if len(key) <= 8 {
		return 0
	}
	return math.MaxUint64 - binary.BigEndian.Uint64(key[len(key)-8:])
}

// ParseKey parses the actual key from the key bytes.
func ParseKey(key []byte) []byte {
	if key == nil {
		return nil
	}
	AssertTrue(len(key) > 8)
	return key[:len(key)-8]
}
