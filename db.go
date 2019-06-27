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

package wisckey

import (
	"math"

	"github.com/shimingyah/wisckey/table"
	"github.com/shimingyah/wisckey/util"
)

// DB provides the thread-safe various functions required to interact with wisckey.
type DB struct {
	opt Options

	// latest (actively written) in-memory table
	mtab table.MemTable

	// immutable mem table
	immtab []table.MemTable
}

// Open returns a new DB object.
func Open(opt Options) (*DB, error) {
	opt.maxBatchSize = (15 * opt.MaxTableSize) / 100
	opt.maxBatchCount = opt.maxBatchSize / int64(util.MaxNodeSize)

	if opt.ValueThreshold > math.MaxUint16-16 {
		return nil, ErrValueThreshold
	}

	return nil, nil
}
