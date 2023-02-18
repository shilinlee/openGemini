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

package sherlock

type MetricCircle struct {
	data    []int
	sum     int
	dataIdx int
	dataCap int
}

func newMetricCircle(dataCap int) *MetricCircle {
	return &MetricCircle{
		data:    make([]int, 0, dataCap),
		dataCap: dataCap,
	}
}

func (c *MetricCircle) push(dat int) {
	if c.dataCap == 0 {
		return
	}

	if len(c.data) < c.dataCap {
		c.sum += dat
		c.data = append(c.data, dat)
		return
	}

	c.sum += dat - c.data[c.dataIdx]

	c.data[c.dataIdx] = dat
	c.dataIdx = (c.dataIdx + 1) % c.dataCap
}

func (c *MetricCircle) mean() int {
	if len(c.data) == 0 {
		return 0
	}
	return c.sum / len(c.data)
}

func (c *MetricCircle) sequentialData() []int {
	index := c.dataIdx
	slice := make([]int, c.dataCap)
	if index == 0 {
		copy(slice, c.data)
		return slice
	}
	copy(slice, c.data[index:])
	copy((slice)[c.dataCap-index:], c.data[:index])
	return slice
}
