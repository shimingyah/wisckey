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

// +build windows

package util

import (
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Mmap use the mmap system call to memory mapped file or device.
// reference badger mmap_windows mmap impl
func Mmap(fd *os.File, offset, length int64, writable bool) ([]byte, error) {
	protect := windows.PAGE_READONLY
	access := windows.FILE_MAP_READ
	if writable {
		protect = windows.PAGE_READWRITE
		access = windows.FILE_MAP_WRITE
	}

	fi, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	// Truncate the file to the size of the mmap.
	if fi.Size() < length {
		if err := fd.Truncate(length); err != nil {
			return nil, err
		}
	}

	// Open a file mapping handle.
	sizelow := uint32(length >> 32)
	sizehigh := uint32(length) & 0xffffffff

	handler, errno := windows.CreateFileMapping(windows.Handle(fd.Fd()), nil, uint32(protect), sizelow, sizehigh, nil)
	if err != nil {
		return nil, os.NewSyscallError("CreateFileMapping", errno)
	}

	// Create the memory map.
	addr, err := windows.MapViewOfFile(handler, uint32(access), 0, 0, uintptr(length))
	if addr == 0 {
		return nil, os.NewSyscallError("MapViewOfFile", err)
	}

	// Slice memory layout
	var sl = struct {
		addr uintptr
		len  int
		cap  int
	}{addr, int(length), int(length)}

	// Use unsafe to turn sl into a []byte.
	data := *(*[]byte)(unsafe.Pointer(&sl))

	return data, nil
}

// Madvise do nothing on windows.
func Madvise(b []byte) error {
	return nil
}

// Lock locks the maped slice, preventing it from being swapped out.
func Lock(b []byte) error {
	err := windows.VirtualLock(uintptr(unsafe.Pointer(&b[0])), len(b))
	return os.NewSyscallError("VirtualLock", err)
}

// Unlock unlocks the mapped slice, allowing it to swap out again.
func Unlock(b []byte) error {
	err := windows.VirtualUnlock(uintptr(unsafe.Pointer(&b[0])), len(b))
	return os.NewSyscallError("VirtualUnlock", err)
}

// Sync flushes mmap slice's all changes back to the device.
// Whether need to call FlushFileBuffers on windows?
func Sync(b []byte) error {
	err := windows.FlushViewOfFile(uintptr(unsafe.Pointer(&b[0])), len(b))
	return os.NewSyscallError("FlushViewOfFile", err)
}

// Munmap unmaps mapped slice, this will also flush any remaining changes.
func Munmap(b []byte) error {
	if err := Sync(b); err != nil {
		return err
	}
	err := windows.UnmapViewOfFile(uintptr(unsafe.Pointer(&b[0])))
	return os.NewSyscallError("UnmapViewOfFile", err)
}