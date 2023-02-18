// Code generated by tmpl; DO NOT EDIT.
// https://github.com/benbjohnson/tmpl
//
// Source: engine/series_agg_func.gen.go.tmpl

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

package engine

import "github.com/openGemini/openGemini/lib/record"

func floatCountReduce(cv *record.ColVal, values []float64, start, end int) (int, int64, bool) {
	count := int64(cv.ValidCount(start, end))
	return start, count, count == 0
}

func integerCountReduce(cv *record.ColVal, values []int64, start, end int) (int, int64, bool) {
	count := int64(cv.ValidCount(start, end))
	return start, count, count == 0
}

func stringCountReduce(cv *record.ColVal, values []string, start, end int) (int, int64, bool) {
	count := int64(cv.ValidCount(start, end))
	return start, count, count == 0
}

func booleanCountReduce(cv *record.ColVal, values []bool, start, end int) (int, int64, bool) {
	count := int64(cv.ValidCount(start, end))
	return start, count, count == 0
}

func integerCountMerge(prevBuf, currBuf *integerColBuf) {
	prevBuf.value += currBuf.value
}

func floatSumReduce(cv *record.ColVal, values []float64, start, end int) (int, float64, bool) {
	var sum float64
	var aggregated int
	if cv.Length()+cv.NilCount == 0 {
		return start, 0, aggregated == 0
	}
	start, end = cv.GetValIndexRange(start, end)
	for _, v := range values[start:end] {
		sum += v
		aggregated++
	}
	return start, sum, aggregated == 0
}

func floatSumMerge(prevBuf, currBuf *floatColBuf) {
	prevBuf.value += currBuf.value
}

func integerSumReduce(cv *record.ColVal, values []int64, start, end int) (int, int64, bool) {
	var sum int64
	var aggregated int
	if cv.Length()+cv.NilCount == 0 {
		return start, 0, aggregated == 0
	}
	start, end = cv.GetValIndexRange(start, end)
	for _, v := range values[start:end] {
		sum += v
		aggregated++
	}
	return start, sum, aggregated == 0
}

func integerSumMerge(prevBuf, currBuf *integerColBuf) {
	prevBuf.value += currBuf.value
}

func floatMinReduce(cv *record.ColVal, values []float64, start, end int) (int, float64, bool) {
	minValue, minIndex := cv.MinFloatValue(values, start, end)
	if minIndex == -1 {
		return 0, 0, true
	}
	return minIndex, minValue, false
}

func floatMinMerge(prevBuf, currBuf *floatColBuf) {
	if currBuf.value < prevBuf.value {
		prevBuf.index = currBuf.index
		prevBuf.time = currBuf.time
		prevBuf.value = currBuf.value
	}
}

func integerMinReduce(cv *record.ColVal, values []int64, start, end int) (int, int64, bool) {
	minValue, minIndex := cv.MinIntegerValue(values, start, end)
	if minIndex == -1 {
		return 0, 0, true
	}
	return minIndex, minValue, false
}

func integerMinMerge(prevBuf, currBuf *integerColBuf) {
	if currBuf.value < prevBuf.value {
		prevBuf.index = currBuf.index
		prevBuf.time = currBuf.time
		prevBuf.value = currBuf.value
	}
}

func booleanMinReduce(cv *record.ColVal, values []bool, start, end int) (int, bool, bool) {
	minValue, minIndex := cv.MinBooleanValue(values, start, end)
	if minIndex == -1 {
		return 0, false, true
	}
	return minIndex, minValue, false
}

func booleanMinMerge(prevBuf, currBuf *booleanColBuf) {
	if currBuf.value != prevBuf.value && !currBuf.value {
		prevBuf.index = currBuf.index
		prevBuf.time = currBuf.time
		prevBuf.value = currBuf.value
	}
}

func floatMaxReduce(cv *record.ColVal, values []float64, start, end int) (int, float64, bool) {
	maxValue, maxIndex := cv.MaxFloatValue(values, start, end)
	if maxIndex == -1 {
		return 0, 0, true
	}
	return maxIndex, maxValue, false
}

func floatMaxMerge(prevBuf, currBuf *floatColBuf) {
	if currBuf.value > prevBuf.value {
		prevBuf.index = currBuf.index
		prevBuf.time = currBuf.time
		prevBuf.value = currBuf.value
	}
}

func integerMaxReduce(cv *record.ColVal, values []int64, start, end int) (int, int64, bool) {
	maxValue, maxIndex := cv.MaxIntegerValue(values, start, end)
	if maxIndex == -1 {
		return 0, 0, true
	}
	return maxIndex, maxValue, false
}

func integerMaxMerge(prevBuf, currBuf *integerColBuf) {
	if currBuf.value > prevBuf.value {
		prevBuf.index = currBuf.index
		prevBuf.time = currBuf.time
		prevBuf.value = currBuf.value
	}
}

func booleanMaxReduce(cv *record.ColVal, values []bool, start, end int) (int, bool, bool) {
	maxValue, maxIndex := cv.MaxBooleanValue(values, start, end)
	if maxIndex == -1 {
		return 0, false, true
	}
	return maxIndex, maxValue, false
}

func booleanMaxMerge(prevBuf, currBuf *booleanColBuf) {
	if currBuf.value != prevBuf.value && currBuf.value {
		prevBuf.index = currBuf.index
		prevBuf.time = currBuf.time
		prevBuf.value = currBuf.value
	}
}

func floatFirstReduce(cv *record.ColVal, values []float64, start, end int) (int, float64, bool) {
	firstValue, firstIndex := cv.FirstFloatValue(values, start, end)
	if firstIndex == -1 {
		return 0, 0, true
	}
	return firstIndex, firstValue, false
}

func floatFirstMerge(prevBuf, currBuf *floatColBuf) {
}

func integerFirstReduce(cv *record.ColVal, values []int64, start, end int) (int, int64, bool) {
	firstValue, firstIndex := cv.FirstIntegerValue(values, start, end)
	if firstIndex == -1 {
		return 0, 0, true
	}
	return firstIndex, firstValue, false
}

func integerFirstMerge(prevBuf, currBuf *integerColBuf) {
}

func stringFirstReduce(cv *record.ColVal, values []string, start, end int) (int, string, bool) {
	firstValue, firstIndex := cv.FirstStringValue(values, start, end)
	if firstIndex == -1 {
		return 0, "", true
	}
	return firstIndex, firstValue, false
}

func stringFirstMerge(prevBuf, currBuf *stringColBuf) {
}

func booleanFirstReduce(cv *record.ColVal, values []bool, start, end int) (int, bool, bool) {
	firstValue, firstIndex := cv.FirstBooleanValue(values, start, end)
	if firstIndex == -1 {
		return 0, false, true
	}
	return firstIndex, firstValue, false
}

func booleanFirstMerge(prevBuf, currBuf *booleanColBuf) {
}

// note: last is designed in ascending order.
func floatLastReduce(cv *record.ColVal, values []float64, start, end int) (int, float64, bool) {
	lastValue, lastIndex := cv.LastFloatValue(values, start, end)
	if lastIndex == -1 {
		return 0, 0, true
	}
	return lastIndex, lastValue, false
}

func floatLastMerge(prevBuf, currBuf *floatColBuf) {
	prevBuf.assign(currBuf)
}

// note: last is designed in ascending order.
func integerLastReduce(cv *record.ColVal, values []int64, start, end int) (int, int64, bool) {
	lastValue, lastIndex := cv.LastIntegerValue(values, start, end)
	if lastIndex == -1 {
		return 0, 0, true
	}
	return lastIndex, lastValue, false
}

func integerLastMerge(prevBuf, currBuf *integerColBuf) {
	prevBuf.assign(currBuf)
}

// note: last is designed in ascending order.
func stringLastReduce(cv *record.ColVal, values []string, start, end int) (int, string, bool) {
	lastValue, lastIndex := cv.LastStringValue(values, start, end)
	if lastIndex == -1 {
		return 0, "", true
	}
	return lastIndex, lastValue, false
}

func stringLastMerge(prevBuf, currBuf *stringColBuf) {
	prevBuf.assign(currBuf)
}

// note: last is designed in ascending order.
func booleanLastReduce(cv *record.ColVal, values []bool, start, end int) (int, bool, bool) {
	lastValue, lastIndex := cv.LastBooleanValue(values, start, end)
	if lastIndex == -1 {
		return 0, false, true
	}
	return lastIndex, lastValue, false
}

func booleanLastMerge(prevBuf, currBuf *booleanColBuf) {
	prevBuf.assign(currBuf)
}

func floatCmpByValueBottom(a, b *FloatPointItem) bool {
	if a.value != b.value {
		return a.value > b.value
	}
	return a.time > b.time
}
func floatCmpByTimeBottom(a, b *FloatPointItem) bool {
	if a.time != b.time {
		return a.time < b.time
	}
	return a.value < b.value
}

func integerCmpByValueBottom(a, b *IntegerPointItem) bool {
	if a.value != b.value {
		return a.value > b.value
	}
	return a.time > b.time
}
func integerCmpByTimeBottom(a, b *IntegerPointItem) bool {
	if a.time != b.time {
		return a.time < b.time
	}
	return a.value < b.value
}

func floatCmpByValueTop(a, b *FloatPointItem) bool {
	if a.value != b.value {
		return a.value < b.value
	}
	return a.time > b.time
}
func floatCmpByTimeTop(a, b *FloatPointItem) bool {
	if a.time != b.time {
		return a.time < b.time
	}
	return a.value > b.value
}

func integerCmpByValueTop(a, b *IntegerPointItem) bool {
	if a.value != b.value {
		return a.value < b.value
	}
	return a.time > b.time
}
func integerCmpByTimeTop(a, b *IntegerPointItem) bool {
	if a.time != b.time {
		return a.time < b.time
	}
	return a.value > b.value
}
