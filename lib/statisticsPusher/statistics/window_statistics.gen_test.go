// Code generated by tmpl; DO NOT EDIT.
// https://github.com/benbjohnson/tmpl
//
// Source: statistics_test.tmpl

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

package statistics_test

import (
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/stretchr/testify/assert"
)

func TestStreamWindow(t *testing.T) {
	stat := statistics.NewStreamWindowStatistics()
	tags := map[string]string{"hostname": "127.0.0.1:8866", "mst": "stream_window"}
	stat.Init(tags)

	fields := map[string]interface{}{
		"window": "",
	}
	statistics.NewTimestamp().Init(time.Second)
	buf, err := stat.Collect(nil)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if err := compareBuffer("stream_window", tags, fields, buf); err != nil {
		t.Fatalf("%v", err)
	}

	item := &statistics.StreamWindowStatItem{}
	item.AddWindowFlushWriteCost(0)
	item.AddWindowFlushMarshalCost(0)
	item.AddWindowSkip(0)
	stat.Push(item)

	buf, err = stat.Collect(nil)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if err := compareBuffer("stream_window", tags, fields, buf); err != nil {
		t.Fatalf("%v", err)
	}
	ops := stat.CollectOps()
	assert.Equal(t, len(ops) == 1, true)
}
