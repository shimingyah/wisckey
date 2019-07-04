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

// Options are params for creating DB object.
//
// This package provides DefaultOptions which contains options that should
// work for most applications. Consider using that as a starting point before
// customizing it for your own needs.
type Options struct {
	// Directory to store the lsm data in. If it doesn't exist, Wisckey will
	// try to create it for you.
	LSMDir string

	// File or device to store the value log in.
	VLogPath string

	// Sync all writes to disk. Setting this to false would achieve better
	// performance, but may cause data to be lost.
	Sync bool

	// How many versions to keep per key.
	NumVersions int

	// Open the DB as read-only. With this set, multiple processes can
	// open the same Wisckey DB. Note: if the DB being opened had crashed
	// before and has vlog data to be replayed, ReadOnly will cause Open
	// to fail with an appropriate message.
	ReadOnly bool

	// DB-specific logger which will override the global logger.
	Logger Logger

	// Each table (or file) is at most this size.
	MaxTableSize int64

	// Maximum number of levels of compaction.
	MaxLevels int

	// If value size >= this threshold, only store value offsets in lsm tree.
	ValueThreshold int

	// Maximum number of tables to keep in memory, before stalling.
	NumMemtables int

	// Maximum number of Level 0 tables before we start compacting.
	NumLevelZeroTables int

	// If we hit this number of Level 0 tables, we will stall until L0 is
	// compacted away.
	NumLevelZeroTablesStall int

	// Maximum total size for L1.
	LevelOneSize int64

	// Number of compaction workers to run concurrently. Setting this to zero would stop compactions
	// to happen within LSM tree. If set to zero, writes could block forever.
	NumCompactors int

	// max entries in batch
	maxBatchCount int64

	// max batch size in bytes
	maxBatchSize int64
}

// DefaultOptions sets a list of recommended options for good performance.
// Feel free to modify these to suit your needs.
var DefaultOptions = Options{
	Sync:               true,
	NumVersions:        1,
	ReadOnly:           false,
	Logger:             nil,
	MaxTableSize:       64 << 20,
	MaxLevels:          7,
	ValueThreshold:     512,
	NumMemtables:       5,
	NumLevelZeroTables: 5,
	NumLevelZeroTablesStall:10,
	LevelOneSize:       256 << 20,
	NumCompactors:      2,
}
