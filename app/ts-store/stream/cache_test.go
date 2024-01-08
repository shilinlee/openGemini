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

package stream_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/openGemini/openGemini/app/ts-store/stream"
)

func Benchmark_WindowDataPool(t *testing.B) {
	pool := stream.NewWindowDataPool()
	for i := 0; i < t.N; i++ {
		for i := 0; i < 10000000; i++ {
			c := &stream.WindowCache{}
			pool.Put(c)
			pool.Get()
		}
	}
}

func Test_WindowDataPool_Len(t *testing.T) {
	pool := stream.NewWindowDataPool()
	c := &stream.WindowCache{}
	pool.Put(c)
	if pool.Len() != 1 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 1, pool.Len()))
	}
	pool.Get()
	if pool.Len() != 0 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 0, pool.Len()))
	}
	pool.Put(c)
	if pool.Len() != 1 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 1, pool.Len()))
	}
	pool.Put(c)
	if pool.Len() != 2 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 2, pool.Len()))
	}
}

func Benchmark_WindowCachePool(t *testing.B) {
	pool := stream.NewWindowCachePool()
	for i := 0; i < t.N; i++ {
		for i := 0; i < 10000000; i++ {
			c := &stream.WindowCache{}
			pool.Put(c)
			pool.Get()
		}
	}
}

func Test_WindowDataPool_Block(t *testing.T) {
	pool := stream.NewWindowDataPool()
	timer := time.NewTicker(1 * time.Second)
	r := make(chan struct{}, 1)
	go func() {
		pool.Get()
		r <- struct{}{}
	}()
	select {
	case <-timer.C:
	case <-r:
		t.Fatal("data pool should block when no data ")
	}
}

func Test_WindowDataPool_NIL(t *testing.T) {
	pool := stream.NewWindowDataPool()
	pool.Put(nil)
	r := pool.Get()
	if r != nil {
		t.Error(fmt.Sprintf("expect %v ,got %v", nil, r))
	}
}

func Test_WindowCachePool_Block(t *testing.T) {
	pool := stream.NewWindowCachePool()
	timer := time.NewTicker(1 * time.Second)
	r := make(chan struct{}, 1)
	go func() {
		pool.Get()
		r <- struct{}{}
	}()
	select {
	case <-timer.C:
		t.Fatal("cache pool should not block when no data ")
	case <-r:
	}
}

func Test_builderPool(t *testing.T) {
	bp := stream.NewBuilderPool()
	sb := bp.Get()
	for i := 0; i < 100; i++ {
		sb.AppendString("xx")
	}
	f := sb.NewString()
	if len(f) != 200 {
		t.Error("len fail")
	}
	sb.Reset()
	bp.Put(sb)
	sb1 := bp.Get()
	for i := 0; i < 100; i++ {
		sb1.AppendString("xx")
	}
	f = sb1.NewString()
	if len(f) != 200 {
		t.Error("len fail")
	}
}

func Test_CacheRowPool_Len(t *testing.T) {
	pool := stream.NewCacheRowPool()
	c1 := pool.Get()
	if pool.Len() != 0 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 0, pool.Len()))
	}
	if pool.Size() != 1 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 1, pool.Size()))
	}
	c2 := pool.Get()
	if pool.Len() != 0 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 0, pool.Len()))
	}
	if pool.Size() != 2 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 2, pool.Size()))
	}
	pool.Put(c1)
	if pool.Len() != 1 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 1, pool.Len()))
	}
	if pool.Size() != 2 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 2, pool.Size()))
	}
	pool.Put(c2)
	if pool.Len() != 2 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 2, pool.Len()))
	}
	if pool.Size() != 2 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 2, pool.Size()))
	}
}

func Test_BuilderPool_Len(t *testing.T) {
	pool := stream.NewBuilderPool()
	c1 := pool.Get()
	if pool.Len() != 0 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 0, pool.Len()))
	}
	if pool.Size() != 1 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 1, pool.Size()))
	}
	c2 := pool.Get()
	if pool.Len() != 0 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 0, pool.Len()))
	}
	if pool.Size() != 2 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 2, pool.Size()))
	}
	pool.Put(c1)
	if pool.Len() != 1 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 1, pool.Len()))
	}
	if pool.Size() != 2 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 2, pool.Size()))
	}
	pool.Put(c2)
	if pool.Len() != 2 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 2, pool.Len()))
	}
	if pool.Size() != 2 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 2, pool.Size()))
	}
}

func Test_StringBuilder(t *testing.T) {
	sb := stream.StringBuilder{}
	sb.AppendString("xx")
	str := sb.String()
	strNew := sb.NewString()
	sb.Reset()
	sb.AppendString("aa")
	str1 := sb.NewString()
	if strNew != "xx" {
		t.Fatal("unexpect", strNew)
	}
	if str != str1 {
		t.Fatal("unexpect", str)
	}
	sb.Reset()
	str2 := sb.NewString()
	if str2 != "" {
		t.Fatal("unexpect", str2)
	}
}

func BenchmarkStringBuilder(t *testing.B) {
	t.ReportAllocs()
	t.ResetTimer()
	sb := stream.StringBuilder{}
	for i := 0; i < t.N; i++ {
		for j := 0; j < 10000000; j++ {
			sb.AppendString("key12345")
		}
	}
}

func BenchmarkBytesBuffer(t *testing.B) {
	t.ReportAllocs()
	t.ResetTimer()
	bb := bytes.Buffer{}
	for i := 0; i < t.N; i++ {
		for j := 0; j < 10000000; j++ {
			bb.WriteString("key12345")
		}
	}
}
