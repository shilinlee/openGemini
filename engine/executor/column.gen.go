// Code generated by tmpl; DO NOT EDIT.
// https://github.com/benbjohnson/tmpl
//
// Source: column.gen.go.tmpl

/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package executor

import (
	"strings"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
)

var (
	_ Column = &ColumnImpl{}
)

type Column interface {
	DataType() influxql.DataType
	Length() int
	NilCount() int
	IsEmpty() bool

	ColumnTime(int) int64
	ColumnTimes() []int64
	AppendColumnTimes(...int64)
	SetColumnTimes([]int64)

	IsNilV2(int) bool
	NilsV2() *Bitmap
	SetNilsBitmap(new *Bitmap)
	AppendNil()
	AppendNilsV2(dat ...bool)
	AppendManyNotNil(num int)
	AppendManyNil(num int)
	GetValueIndexV2(start int) int
	GetRangeValueIndexV2(bmStart, bmEnd int) (s int, e int)
	GetTimeIndex(valIdx int) int
	Reset()

	FloatValue(int) float64
	FloatValues() []float64
	AppendFloatValues(...float64)
	SetFloatValues([]float64)

	IntegerValue(int) int64
	IntegerValues() []int64
	AppendIntegerValues(...int64)
	SetIntegerValues([]int64)

	StringValue(int) string
	StringValuesV2(dst []string) []string
	StringValuesRange(dst []string, start, end int) []string
	AppendStringValues(...string)
	AppendStringBytes([]byte, []uint32)
	SetStringValues([]byte, []uint32)
	GetStringBytes() ([]byte, []uint32)
	CloneStringValues([]byte, []uint32)
	GetStringValueBytes(valueBits []byte, value []string, start, end int) ([]byte, []string)

	BooleanValue(int) bool
	BooleanValues() []bool
	AppendBooleanValues(...bool)
	SetBooleanValues([]bool)

	//TODO:CheckColumn used to check the chunk's structure
	// Remember to remove it!
	CheckColumn(int)

	Marshal([]byte) ([]byte, error)
	Unmarshal([]byte) error
	Size() int
	BitMap() *Bitmap
}

type ColumnImpl struct {
	dataType      influxql.DataType
	floatValues   []float64
	integerValues []int64
	stringBytes   []byte
	offset        []uint32
	booleanValues []bool
	times         []int64
	nilsV2        *Bitmap
}

func NewColumnImpl(dataType influxql.DataType) *ColumnImpl {
	return &ColumnImpl{
		dataType: dataType,
		nilsV2:   NewBitmap(),
	}
}

func (c *ColumnImpl) DataType() influxql.DataType {
	return c.dataType
}

func (c *ColumnImpl) Length() int {
	return c.nilsV2.length
}
func (c *ColumnImpl) NilCount() int {
	return c.nilsV2.nilCount
}
func (c *ColumnImpl) IsEmpty() bool {
	return c.NilCount() == c.Length()
}

func (c *ColumnImpl) ColumnTime(idx int) int64 {
	return c.times[idx]
}

func (c *ColumnImpl) ColumnTimes() []int64 {
	return c.times
}

func (c *ColumnImpl) AppendColumnTimes(values ...int64) {
	c.times = append(c.times, values...)
}

func (c *ColumnImpl) SetColumnTimes(values []int64) {
	c.times = values
}

func (c *ColumnImpl) Time(idx int) int64 {
	return c.times[idx]
}

func (c *ColumnImpl) Times() []int64 {
	return c.times
}

func (c *ColumnImpl) AppendTimes(times ...int64) {
	c.times = append(c.times, times...)
}

func (c *ColumnImpl) IsNilV2(idx int) bool {
	return !c.nilsV2.containsInt(idx)
}

func (c *ColumnImpl) NilsV2() *Bitmap {
	return c.nilsV2
}

// SetNilsBitmap just for test use now.
func (c *ColumnImpl) SetNilsBitmap(new *Bitmap) {
	c.nilsV2 = new
}

func (c *ColumnImpl) AppendNil() {
	c.nilsV2.append(false)
}
func (c *ColumnImpl) AppendNilsV2(dat ...bool) {
	if len(dat) == 1 {
		c.nilsV2.append(dat[0])
		return
	}
	c.nilsV2.appendManyV2(dat)
}

func (c *ColumnImpl) GetValueIndexV2(start int) int {
	rankS := c.nilsV2.rank(start)
	return rankS
}

func (c *ColumnImpl) GetRangeValueIndexV2(bmStart, bmEnd int) (int, int) {
	rankS := c.nilsV2.rank(bmStart)
	rankE := c.nilsV2.rank(bmEnd)
	return rankS, rankE
}

func (c *ColumnImpl) AppendManyNotNil(num int) {
	for i := 0; i < num; i++ {
		c.AppendNilsV2(true)
	}
}

func (c *ColumnImpl) AppendManyNil(num int) {
	if c.NilCount() == 0 && c.Length() == 0 {
		n := num >> 3
		if num%8 != 0 {
			n += 1
		}
		c.nilsV2.bits = append(c.nilsV2.bits, make([]byte, n)...)
		c.nilsV2.length += num
		c.nilsV2.nilCount += num
		return
	}
	for i := 0; i < num; i++ {
		c.AppendNilsV2(false)
	}
}

func (c *ColumnImpl) GetTimeIndex(valIdx int) int {
	return int(c.nilsV2.array[valIdx])
}

func (c *ColumnImpl) Reset() {
	c.floatValues = c.floatValues[:0]
	c.integerValues = c.integerValues[:0]
	c.stringBytes = c.stringBytes[:0]
	c.offset = c.offset[:0]
	c.booleanValues = c.booleanValues[:0]
	c.times = c.times[:0]
	c.nilsV2.Clear()
}

func (c *ColumnImpl) FloatValue(idx int) float64 {
	return c.floatValues[idx]
}

func (c *ColumnImpl) FloatValues() []float64 {
	return c.floatValues
}

func (c *ColumnImpl) AppendFloatValues(values ...float64) {
	c.floatValues = append(c.floatValues, values...)
}

func (c *ColumnImpl) SetFloatValues(values []float64) {
	c.floatValues = values
}

func (c *ColumnImpl) IntegerValue(idx int) int64 {
	return c.integerValues[idx]
}

func (c *ColumnImpl) IntegerValues() []int64 {
	return c.integerValues
}

func (c *ColumnImpl) AppendIntegerValues(values ...int64) {
	c.integerValues = append(c.integerValues, values...)
}

func (c *ColumnImpl) SetIntegerValues(values []int64) {
	c.integerValues = values
}

// String type

func (c *ColumnImpl) StringValue(idx int) string {
	if idx == len(c.offset)-1 {
		off := c.offset[idx]
		return record.Bytes2str(c.stringBytes[off:])
	} else {
		start := c.offset[idx]
		end := c.offset[idx+1]
		return record.Bytes2str(c.stringBytes[start:end])
	}
}

// StringValuesV2 just use for test.
func (c *ColumnImpl) StringValuesV2(dst []string) []string {
	if len(c.offset) == 0 {
		return dst
	}

	offs := c.offset
	for i := 0; i < len(offs); i++ {
		off := offs[i]
		if i == len(offs)-1 {
			dst = append(dst, record.Bytes2str(c.stringBytes[off:]))
		} else {
			dst = append(dst, record.Bytes2str(c.stringBytes[off:offs[i+1]]))
		}
	}
	return dst
}

func (c *ColumnImpl) StringValuesRange(dst []string, start, end int) []string {
	offs := c.offset
	for i := start; i < end; i++ {
		off := offs[i]
		if i == len(offs)-1 {
			dst = append(dst, record.Bytes2str(c.stringBytes[off:]))
		} else {
			dst = append(dst, record.Bytes2str(c.stringBytes[off:offs[i+1]]))
		}
	}

	return dst
}

// Deprecated: please do not use. recommend to use AppendStringBytes
func (c *ColumnImpl) AppendStringValues(values ...string) {
	for _, value := range values {
		b := record.Str2bytes(value)
		c.offset = append(c.offset, uint32(len(c.stringBytes)))
		c.stringBytes = append(c.stringBytes, b...)
	}
}

func (c *ColumnImpl) AppendStringBytes(val []byte, offset []uint32) {
	if len(val) == 0 {
		return
	}
	sbLen := uint32(len(c.stringBytes))
	for _, off := range offset {
		c.offset = append(c.offset, off+sbLen)
	}
	c.stringBytes = append(c.stringBytes, val...)
}

func (c *ColumnImpl) SetStringValues(val []byte, offset []uint32) {
	c.stringBytes = val
	c.offset = offset
}

func (c *ColumnImpl) GetStringBytes() ([]byte, []uint32) {
	return c.stringBytes, c.offset
}

func (c *ColumnImpl) CloneStringValues(val []byte, offset []uint32) {
	c.stringBytes = make([]byte, len(val))
	c.offset = make([]uint32, len(offset))
	copy(c.stringBytes, val)
	copy(c.offset, offset)
}

func (c *ColumnImpl) GetStringValueBytes(valueBits []byte, value []string, start, end int) ([]byte, []string) {
	var os, oe int
	stringBytes, offset := c.GetStringBytes()
	os = int(offset[start])
	if end < len(offset) {
		oe = int(offset[end])
	} else {
		oe = len(stringBytes)
	}
	vbl := len(valueBits)
	valueBits = bufferpool.Resize(valueBits, vbl+oe-os)
	for i := start; i < end; i++ {
		oriStr := c.StringValue(i)
		vs := vbl + int(offset[i]-offset[start])
		ve := vs + len(oriStr)
		newStr := valueBits[vs:ve]
		copy(newStr, oriStr)
		value = append(value, record.Bytes2str(newStr))
	}
	return valueBits, value
}

func (c *ColumnImpl) BitMap() *Bitmap {
	return c.nilsV2
}

func (c *ColumnImpl) BooleanValue(idx int) bool {
	return c.booleanValues[idx]
}

func (c *ColumnImpl) BooleanValues() []bool {
	return c.booleanValues
}

func (c *ColumnImpl) AppendBooleanValues(values ...bool) {
	c.booleanValues = append(c.booleanValues, values...)
}

func (c *ColumnImpl) SetBooleanValues(values []bool) {
	c.booleanValues = values
}

func (c *ColumnImpl) CheckColumn(length int) {
	switch c.dataType {
	case influxql.String, influxql.Tag:
		if len(c.integerValues) != 0 || len(c.floatValues) != 0 || len(c.booleanValues) != 0 {
			panic("Row in chunk check failed: it has wrong datatype, the row's dataType should be string!")
		}
		if c.NilCount()+len(c.offset) != length {
			panic("Row in chunk check failed: the number of the data(include nil data) doesn't fit chunk length!")
		}
	case influxql.Float:
		if len(c.integerValues) != 0 || len(c.stringBytes) != 0 || len(c.booleanValues) != 0 {
			panic("Row in chunk check failed: it has wrong datatype, the row's dataType should be float64!")
		}
		if c.NilCount()+len(c.floatValues) != length {
			panic("Row in chunk check failed: the number of the data(include nil data) doesn't fit chunk length!")
		}
	case influxql.Integer:
		if len(c.floatValues) != 0 || len(c.stringBytes) != 0 || len(c.booleanValues) != 0 {
			panic("Row in chunk check failed: it has wrong datatype, the row's dataType should be int64!")
		}
		if c.NilCount()+len(c.integerValues) != length {
			panic("Row in chunk check failed: the number of the data(include nil data) doesn't fit chunk length!")
		}
	case influxql.Boolean:
		if len(c.integerValues) != 0 || len(c.stringBytes) != 0 || len(c.floatValues) != 0 {
			panic("Row in chunk check failed: it has wrong datatype, the row's dataType should be boolean!")
		}
		if c.NilCount()+len(c.booleanValues) != length {
			panic("Row in chunk check failed: the number of the data(include nil data) doesn't fit chunk length!")
		}
	}
}

const (
	bitSize = 8
)

var (
	bitmask  = [8]byte{1 << 7, 1 << 6, 1 << 5, 1 << 4, 1 << 3, 1 << 2, 1 << 1, 1}
	BitMask2 = [8]byte{1, 2, 4, 8, 16, 32, 64, 128}
)

type Byte byte

func (by Byte) isZero(x int) bool {
	return byte(by)&bitmask[x] == 0
}

// Bitmap for column
type Bitmap struct {
	bits     []byte
	array    []uint16 // Valued Index
	length   int      // same as len(ck.times).
	nilCount int      // the nil num in the bits.
}

func NewBitmap() *Bitmap {
	return &Bitmap{
		bits: make([]byte, 0),
	}
}

func (b *Bitmap) CopyTo(dst *Bitmap) {
	if cap(dst.bits) < len(b.bits) {
		dst.bits = make([]byte, len(b.bits))
	} else {
		dst.bits = dst.bits[:len(b.bits)]
	}
	copy(dst.bits, b.bits)
	if len(b.array) > 0 {
		if cap(dst.array) < len(b.array) {
			dst.array = make([]uint16, len(b.array))
		} else {
			dst.array = dst.array[:len(b.array)]
		}
		copy(dst.array, b.array)
	}
	dst.length = b.length
	dst.nilCount = b.nilCount
}

func (b *Bitmap) Clear() {
	b.bits = b.bits[:0]
	b.array = b.array[:0]
	b.length = 0
	b.nilCount = 0
}

func (b *Bitmap) fixArray() {
	// all nil
	if b.nilCount == b.length {
		return
	}
	// all not nil
	b.array = make([]uint16, 0, b.length)
	for i := 0; i < b.length; i++ {
		b.array = append(b.array, uint16(i))
	}
}

func (b *Bitmap) UpdateBitWithArray(dat []bool) {
	b.bits = b.bits[:0]

	// init the dat
	for _, v := range b.array {
		dat[v] = true
	}

	// the front bytes
	byteN := b.length / bitSize
	for i := 0; i < byteN; i++ {
		var bt byte
		for j := 0; j < bitSize; j++ {
			if dat[i*bitSize+j] {
				bt = bt<<1 + 1
			} else {
				bt = bt << 1
			}
		}
		b.bits = append(b.bits, bt)
	}

	// the last byte
	if offset := b.length % bitSize; offset > 0 {
		var bt byte
		for k := 0; k < offset; k++ {
			if dat[byteN*bitSize+k] {
				bt = bt<<1 + 1
			} else {
				bt = bt << 1
			}
		}
		bt = bt << (bitSize - offset)
		b.bits = append(b.bits, bt)
	}

	// reset the dat
	for _, v := range b.array {
		dat[v] = false
	}
}

// append
func (b *Bitmap) append(dat bool) {
	if !dat && len(b.array) == 0 {
		b.fixArray()
	}
	if offset := b.length % bitSize; offset > 0 {
		slot := bitSize - offset
		var bt = b.bits[len(b.bits)-1]
		bt = bt >> slot
		if dat {
			if bt == 0 {
				bt = 1 << (slot - 1)
				bt = bt >> (slot - 1)
			} else {
				bt = bt<<1 + 1
			}
			if b.nilCount > 0 {
				b.array = append(b.array, uint16(b.length))
			}
		} else {
			bt = bt << 1
			b.nilCount++
		}
		bt = bt << (slot - 1)
		b.bits[len(b.bits)-1] = bt
	} else {
		if dat {
			b.bits = append(b.bits, 1<<7)
			if b.nilCount > 0 {
				b.array = append(b.array, uint16(b.length))
			}
		} else {
			b.bits = append(b.bits, 0)
			b.nilCount++
		}
	}
	b.length++
}

func (b *Bitmap) appendManyV2(dat []bool) {
	if offset := b.length % bitSize; offset != 0 {
		slot := bitSize - offset
		var bt = b.bits[len(b.bits)-1]
		bt = bt >> slot
		for i := 0; i < slot; i++ {
			if len(dat) > 0 {
				if dat[0] {
					bt = bt<<1 + 1
					if b.nilCount > 0 {
						b.array = append(b.array, uint16(b.length))
					}
				} else {
					bt = bt << 1
					if len(b.array) == 0 {
						b.fixArray()
					}
					b.nilCount++
				}
				b.length++
				dat = dat[1:]
			} else {
				bt = bt << (slot - i)
				break
			}
		}
		b.bits[len(b.bits)-1] = bt
	}

	byteN := len(dat) / bitSize
	for i := 0; i < byteN; i++ {
		var bt byte
		for j := 0; j < bitSize; j++ {
			if dat[i*bitSize+j] {
				bt = bt<<1 + 1
				if b.nilCount > 0 {
					b.array = append(b.array, uint16(b.length))
				}
			} else {
				bt = bt << 1
				if len(b.array) == 0 {
					b.fixArray()
				}
				b.nilCount++
			}
			b.length++
		}
		b.bits = append(b.bits, bt)
	}
	// the last byte
	if offset := len(dat) % bitSize; offset > 0 {
		var bt byte
		for k := 0; k < offset; k++ {
			if dat[byteN*bitSize+k] {
				bt = bt<<1 + 1
				if b.nilCount > 0 {
					b.array = append(b.array, uint16(b.length))
				}
			} else {
				bt = bt << 1
				if len(b.array) == 0 {
					b.fixArray()
				}
				b.nilCount++
			}
			b.length++
		}
		bt = bt << (bitSize - offset)
		b.bits = append(b.bits, bt)
	}
}

func (b *Bitmap) containsInt(x int) bool {
	byteIdx := x >> 3
	if byteIdx >= len(b.bits) {
		return false
	} else if byteIdx < 0 {
		return false
	}
	bitPos := x % bitSize
	theByte := Byte(b.bits[byteIdx])
	return !theByte.isZero(bitPos)
}

// rank returns the number of integers that are smaller or equal to x (Rank(infinity) would be GetCardinality()).
// If you pass the smallest value, you get the value 1. If you pass a value that is smaller than the smallest
// value, you get 0.
func (b *Bitmap) rank(x int) int {
	if x == 0 {
		return 0
	} else if x >= b.length {
		return b.length - b.nilCount
	}

	if b.nilCount == 0 {
		return x
	}
	// binary search
	answer := hybridqp.BinarySearchForNils(b.array, uint16(x))
	if answer >= 0 {
		return answer
	}
	return -answer - 1
}

func (b *Bitmap) ToArray() []uint16 {
	return b.array
}

func (b *Bitmap) ToBit() []byte {
	return b.bits
}

func (b *Bitmap) SetArray(arr []uint16) {
	b.array = b.array[:0]
	b.array = append(b.array, arr...)
}

func (b *Bitmap) SetLen(len int) {
	b.length = len
}

func (b *Bitmap) SetNilCount(nilCount int) {
	b.nilCount = nilCount
}

func reverse(num byte) byte {
	revBits := byte(0)
	for i := 0; i < 8; i++ {
		revBits <<= 1
		revBits |= num >> i & 1
	}
	return revBits
}

func (b *Bitmap) Reverse() {
	if offset := b.length % 8; offset == 0 {
		for i := 0; i <= b.length/2; i++ {
			m1, m2 := b.bits[i], b.bits[b.length-i-1]
			b.bits[i], b.bits[b.length-i-1] = reverse(m2), reverse(m1)
		}
	} else {
		bm := NewBitmap()
		b.CopyTo(bm)
		b.Clear()
		for i := bm.length - 1; i >= 0; i-- {
			if bm.containsInt(i) {
				b.append(true)
			} else {
				b.append(false)
			}
		}
	}
}

func (b *Bitmap) String() string {
	var sb strings.Builder
	for i := 0; i < b.length; i++ {
		theByte := Byte(b.bits[i/bitSize])
		if theByte.isZero(i % bitSize) {
			sb.WriteString("false ")
		} else {
			sb.WriteString("true ")
		}
	}
	return sb.String()
}

func UnionBitMapArray(b1, b2 []uint16) []uint16 {
	dst := make([]uint16, 0, len(b1))
	p1, p2 := 0, 0
	len1, len2 := len(b1), len(b2)
mark:
	for (p1 < len1) && (p2 < len2) {
		v1, v2 := b1[p1], b2[p2]

		for {
			if v1 < v2 {
				dst = append(dst, b1[p1])
				p1++
				if p1 == len1 {
					break mark
				}
				v1 = b1[p1]
			} else if v1 > v2 {
				dst = append(dst, b2[p2])
				p2++
				if p2 == len2 {
					break mark
				}
				v2 = b2[p2]
			} else {
				dst = append(dst, b1[p1])
				p1++
				p2++
				if (p1 == len1) || (p2 == len2) {
					break mark
				}
				v1, v2 = b1[p1], b2[p2]
			}
		}
	}
	if p1 == len1 {
		for i := p2; i < len2; i++ {
			dst = append(dst, b2[i])
		}
	} else if p2 == len2 {
		for i := p1; i < len1; i++ {
			dst = append(dst, b1[i])
		}
	}
	return dst
}
