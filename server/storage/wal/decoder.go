// Copyright 2015 The etcd Authors
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
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import (
	"bufio"
	"encoding/binary"
	"hash"
	"io"
	"sync"

	"go.etcd.io/etcd/pkg/v3/crc"
	"go.etcd.io/etcd/pkg/v3/pbutil"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
)

const minSectorSize = 512

// frameSizeBytes is frame size in bytes, including record size and padding size.
const frameSizeBytes = 8

type decoder struct {
	mu  sync.Mutex
	brs []*bufio.Reader

	// lastValidOff file offset following the last valid decoded record
	lastValidOff int64
	crc          hash.Hash32
}

func newDecoder(r ...io.Reader) *decoder {
	readers := make([]*bufio.Reader, len(r))
	for i := range r {
		readers[i] = bufio.NewReader(r[i])
	}
	return &decoder{
		brs: readers,
		crc: crc.New(0, crcTable),
	}
}

// 这是破译记录
func (d *decoder) decode(rec *walpb.Record) error {
	rec.Reset() // 重置
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.decodeRecord(rec) //
}

// raft max message size is set to 1 MB in etcd server
// assume projects set reasonable message size limit,
// thus entry size should never exceed 10 MB
const maxWALEntrySizeLimit = int64(10 * 1024 * 1024)

// 解码记录从文件里面
func (d *decoder) decodeRecord(rec *walpb.Record) error {
	if len(d.brs) == 0 {
		return io.EOF
	}
	// 读取第一个8字节。代表长度
	l, err := readInt64(d.brs[0])
	if err == io.EOF || (err == nil && l == 0) {
		// 此时是说明该reader是空的
		// hit end of file or preallocated space
		d.brs = d.brs[1:] // 用后面的bufferio，第一个不要了
		if len(d.brs) == 0 {
			// 说明只有一个
			return io.EOF
		}
		d.lastValidOff = 0
		// 修改了之后继续decoder。厉害
		return d.decodeRecord(rec)
	}
	if err != nil {
		return err
	}

	// 计算字节数
	recBytes, padBytes := decodeFrameSize(l)
	if recBytes >= maxWALEntrySizeLimit-padBytes {
		return ErrMaxWALEntrySizeLimitExceeded
	}

	data := make([]byte, recBytes+padBytes)
	// 全部读出来
	if _, err = io.ReadFull(d.brs[0], data); err != nil {
		// ReadFull returns io.EOF only if no bytes were read
		// the decoder should treat this as an ErrUnexpectedEOF instead.
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	// 解析成pb的格式，用的是pb的编码格式。
	if err := rec.Unmarshal(data[:recBytes]); err != nil {
		// 还有可能是分裂写？（分裂写说明有的chunk部分全是0
		if d.isTornEntry(data) {
			return io.ErrUnexpectedEOF
		}
		return err
	}

	// skip crc checking if the record type is crcType
	if rec.Type != crcType {
		// 记录==rec==record
		d.crc.Write(rec.Data)
		// 写到hash的校验中
		if err := rec.Validate(d.crc.Sum32()); err != nil { // 这个validate是判断相等与否
			if d.isTornEntry(data) {
				return io.ErrUnexpectedEOF
			}
			return err
		}
	}
	// record decoded as valid; point last valid offset to end of record
	d.lastValidOff += frameSizeBytes + recBytes + padBytes
	return nil
}

func decodeFrameSize(lenField int64) (recBytes int64, padBytes int64) {
	// the record size is stored in the lower 56 bits of the 64-bit length
	recBytes = int64(uint64(lenField) & ^(uint64(0xff) << 56))
	// non-zero padding is indicated by set MSb / a negative length
	if lenField < 0 {
		// padding is stored in lower 3 bits of length MSB
		padBytes = int64((uint64(lenField) >> 56) & 0x7)
	}
	return recBytes, padBytes
}

// isTornEntry determines whether the last entry of the WAL was partially written
// and corrupted because of a torn write.
func (d *decoder) isTornEntry(data []byte) bool {
	if len(d.brs) != 1 {
		return false
	}

	fileOff := d.lastValidOff + frameSizeBytes
	curOff := 0
	chunks := [][]byte{}
	// split data on sector boundaries
	for curOff < len(data) {
		chunkLen := int(minSectorSize - (fileOff % minSectorSize))
		if chunkLen > len(data)-curOff {
			chunkLen = len(data) - curOff
		}
		chunks = append(chunks, data[curOff:curOff+chunkLen])
		fileOff += int64(chunkLen)
		curOff += chunkLen
	}

	// if any data for a sector chunk is all 0, it's a torn write
	// 全是0就是分开写的。
	for _, sect := range chunks {
		isZero := true
		for _, v := range sect {
			if v != 0 {
				isZero = false
				break
			}
		}
		if isZero {
			return true
		}
	}
	return false
}

func (d *decoder) updateCRC(prevCrc uint32) {
	d.crc = crc.New(prevCrc, crcTable)
}

func (d *decoder) lastCRC() uint32 {
	return d.crc.Sum32()
}

func (d *decoder) lastOffset() int64 { return d.lastValidOff }

func mustUnmarshalEntry(d []byte) raftpb.Entry {
	var e raftpb.Entry
	pbutil.MustUnmarshal(&e, d)
	return e
}

func mustUnmarshalState(d []byte) raftpb.HardState {
	var s raftpb.HardState
	pbutil.MustUnmarshal(&s, d)
	return s
}

func readInt64(r io.Reader) (int64, error) {
	var n int64
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, err
}
