/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

func Test_Time_ConsumeData(t *testing.T) {
	t.Skip()
	l := &MockLogger{t}
	m := &MockStorage{}
	metaClient := &MockMetaclient{}

	src := meta.StreamMeasurementInfo{
		Name:            "flow",
		Database:        "test",
		RetentionPolicy: "auto",
	}
	des := meta.StreamMeasurementInfo{
		Name:            "flow1",
		Database:        "test",
		RetentionPolicy: "auto",
	}
	interval := 5 * time.Second
	start := time.Now().Truncate(interval).Add(-interval)
	fieldCalls := []*FieldCall{}
	calls := []string{"sum", "min", "max", "count"}
	for _, c := range calls {
		fieldCalls = append(fieldCalls, &FieldCall{
			name:         "bps",
			alias:        "bps",
			call:         c,
			tagFunc:      nil,
			inFieldType:  influx.Field_Type_Float,
			outFieldType: influx.Field_Type_Float,
		})
	}
	task := &TimeTask{WindowDataPool: NewWindowDataPool(), windowCachePool: NewWindowCachePool(), updateWindow: make(chan struct{}),
		abort: make(chan struct{}), windowNum: 10, stats: statistics.NewStreamWindowStatItem(0),
		Logger: l, store: m, cli: metaClient, src: &src, des: &des, window: interval,
		start: start, end: start.Add(interval), fieldCalls: fieldCalls}
	task.run()
	// wait run
	fieldRows := buildRows(1000)
	now := time.Now()
	for i := 0; i < 10000; i++ {
		cache := task.windowCachePool.Get()
		cache.ptId = 0
		cache.shardId = 0
		cache.rows = fieldRows
		cache.release = nil
		task.Put(cache)
	}
	task.Drain()
	t.Log("cost", time.Now().Sub(now))
	if task.stats.WindowIn != 10000000 {
		t.Fatal("unexpect in", task.stats.WindowIn)
	}
	if task.stats.WindowSkip != 0 {
		t.Fatal("unexpect skip", task.stats.WindowSkip)
	}
	if task.stats.WindowProcess != 10000000 {
		t.Fatal("unexpect process", task.stats.WindowProcess)
	}
	time.Sleep(interval)
	time.Sleep(interval)
	if task.stats.WindowIn != 0 {
		t.Fatal("unexpect in", task.stats.WindowIn)
	}
	if task.stats.WindowSkip != 0 {
		t.Fatal("unexpect skip", task.stats.WindowSkip)
	}
	if task.stats.WindowProcess != 0 {
		t.Fatal("unexpect process", task.stats.WindowProcess)
	}
	time.Sleep(interval)
	if m.count != 1 {
		t.Fatal("unexpect flush count", m.count)
	}
}

func BenchmarkXX(b *testing.B) {
	a := make([]bool, 10)
	a[5] = true
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100000000; j++ {
			a[5] = true
		}
	}
}

func BenchmarkXX1(b *testing.B) {
	a := make([]bool, 10)
	a[5] = true
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100000000; j++ {
			if !a[5] {
				a[5] = true
			}
		}
	}
}
