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

package stream

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

func NewCacheRowPool() *CacheRowPool {
	rowsPool := NewRowsPool()
	p := &CacheRowPool{rowsPool: rowsPool}
	return p
}

type CacheRowPool struct {
	pool     sync.Pool
	size     int64
	length   int64
	rowsPool *RowsPool
}

func (p *CacheRowPool) Get() *CacheRow {
	c := p.pool.Get()
	if c == nil {
		atomic.AddInt64(&p.size, 1)
		return &CacheRow{rows: *p.rowsPool.Get()}
	}
	atomic.AddInt64(&p.length, -1)
	return c.(*CacheRow)
}

func (p *CacheRowPool) Put(r *CacheRow) {
	p.rowsPool.Put(&r.rows)
	r.rows = nil
	r.ww = nil
	p.pool.Put(r)
	atomic.AddInt64(&p.length, 1)
}

func (p *CacheRowPool) Len() int64 {
	return atomic.LoadInt64(&p.length)
}

func (p *CacheRowPool) Size() int64 {
	return atomic.LoadInt64(&p.size)
}

func NewBuilderPool() *BuilderPool {
	p := &BuilderPool{}
	return p
}

type StringBuilder struct {
	buf []byte
}

func (b *StringBuilder) String() string {
	return *(*string)(unsafe.Pointer(&b.buf))
}

func (b *StringBuilder) NewString() string {
	if len(b.buf) == 0 {
		return ""
	}
	s := make([]byte, len(b.buf))
	copy(s, b.buf)
	return *(*string)(unsafe.Pointer(&s))
}

func (b *StringBuilder) Reset() {
	b.buf = b.buf[:0]
}

func (b *StringBuilder) AppendByte(c byte) {
	b.buf = append(b.buf, c)
}

func (b *StringBuilder) AppendString(s string) {
	b.buf = append(b.buf, s...)
}

type BuilderPool struct {
	pool   sync.Pool
	size   int64
	length int64
}

func (p *BuilderPool) Get() *StringBuilder {
	c := p.pool.Get()
	if c == nil {
		atomic.AddInt64(&p.size, 1)
		return &StringBuilder{}
	}
	atomic.AddInt64(&p.length, -1)
	return c.(*StringBuilder)
}

func (p *BuilderPool) Put(r *StringBuilder) {
	p.pool.Put(r)
	atomic.AddInt64(&p.length, 1)
}

func (p *BuilderPool) Len() int64 {
	return atomic.LoadInt64(&p.length)
}

func (p *BuilderPool) Size() int64 {
	return atomic.LoadInt64(&p.size)
}

type RowsPool struct {
	pool sync.Pool
}

func NewRowsPool() *RowsPool {
	p := &RowsPool{}
	return p
}

func (p *RowsPool) Get() *[]influx.Row {
	c := p.pool.Get()
	if c == nil {
		return &[]influx.Row{}
	}
	return c.(*[]influx.Row)
}

func (p *RowsPool) Put(r *[]influx.Row) {
	p.pool.Put(r)
}

type WindowDataPool struct {
	cache  chan *WindowCache
	length int64
}

func NewWindowDataPool() *WindowDataPool {
	n := cpu.GetCpuNum() * 8
	if n < 4 {
		n = 4
	}
	if n > 256 {
		n = 256
	}

	p := &WindowDataPool{
		cache: make(chan *WindowCache, n),
	}
	return p
}

func (p *WindowDataPool) Get() *WindowCache {
	cache := <-p.cache
	p.IncreaseChan()
	return cache
}

func (p *WindowDataPool) IncreaseChan() {
	atomic.AddInt64(&p.length, -1)
}

func (p *WindowDataPool) Put(cache *WindowCache) {
	p.cache <- cache
	atomic.AddInt64(&p.length, 1)
}

func (p *WindowDataPool) Len() int64 {
	return atomic.LoadInt64(&p.length)
}

type WindowCachePool struct {
	pool  sync.Pool
	count int64
}

func NewWindowCachePool() *WindowCachePool {
	p := &WindowCachePool{}
	return p
}

func (p *WindowCachePool) Get() *WindowCache {
	atomic.AddInt64(&p.count, 1)
	c := p.pool.Get()
	if c == nil {
		return &WindowCache{}
	}
	return c.(*WindowCache)
}

func (p *WindowCachePool) Put(r *WindowCache) {
	atomic.AddInt64(&p.count, -1)
	p.pool.Put(r)
}

func (p *WindowCachePool) Count() int64 {
	return atomic.LoadInt64(&p.count)
}
