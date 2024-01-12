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
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	numenc "github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	atomic2 "github.com/openGemini/openGemini/lib/atomic"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/stringinterner"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

var (
	maxWindowNum           = 10
	FlushParallelMinRowNum = 10000
)

const (
	EmptyGroupKey = ""
	EmptyTagValue = ""
)

type TagTask struct {
	// compress dict
	// corpus        map[string]uint64
	corpus        sync.Map
	corpusIndexes []string
	corpusIndex   uint64
	corpusLock    sync.Mutex

	// key tag values
	values sync.Map
	// store startWindow id, for ring store structure
	startWindowID int64
	offset        int
	// current window start time
	start time.Time
	// current window end time
	end time.Time
	// store all ptIds for all window
	ptIds []*uint32
	// store all shardIds for all window
	shardIds []*uint64

	// metadata, not change
	src        *meta2.StreamMeasurementInfo
	des        *meta2.StreamMeasurementInfo
	info       *meta2.MeasurementInfo
	groupKeys  []string
	fieldCalls []*FieldCall

	// chan for process
	innerCache     chan *WindowCache
	innerRes       chan error
	abort          chan struct{}
	err            error
	updateWindow   chan struct{}
	cleanPreWindow chan struct{}

	// pool
	bp              *BuilderPool
	windowCachePool *WindowCachePool
	*WindowDataPool
	indexKeyPool []byte

	// config
	id          uint64
	name        string
	concurrency int
	windowNum   int
	window      time.Duration
	maxDelay    time.Duration

	// tmp data, reuse
	fieldCallsLen int
	rows          []influx.Row
	validNum      int
	maxDuration   int64

	// tools
	flushWG sync.WaitGroup
	goPool  *ants.Pool
	stats   *statistics.StreamWindowStatItem
	store   Storage
	Logger  Logger
	cli     MetaClient
}

type WindowCache struct {
	rows    []influx.Row
	shardId uint64
	ptId    uint32
	release func() bool
}

func (s *TagTask) Put(r *WindowCache) {
	s.WindowDataPool.Put(r)
}

func (s *TagTask) stop() error {
	close(s.abort)
	return s.err
}

func (s *TagTask) getSrcInfo() *meta2.StreamMeasurementInfo {
	return s.src
}

func (s *TagTask) getDesInfo() *meta2.StreamMeasurementInfo {
	return s.des
}

func (s *TagTask) getName() string {
	return s.name
}

func (s *TagTask) run() error {
	err := s.initVar()
	if err != nil {
		s.err = err
		return err
	}
	s.info, err = s.cli.GetMeasurementInfoStore(s.des.Database, s.des.RetentionPolicy, s.des.Name)
	if err != nil {
		s.err = err
		return err
	}
	err = s.buildFieldCalls()
	if err != nil {
		return err
	}
	go s.cycleFlush()
	go s.parallelCalculate()
	go s.cleanWindow()
	go s.consumeDataAndUpdateMeta()
	return nil
}

func (s *TagTask) initVar() error {
	s.maxDuration = int64(s.windowNum) * s.window.Nanoseconds()
	s.abort = make(chan struct{})
	// chan len zero, make updateWindow cannot parallel execute with flush
	s.updateWindow = make(chan struct{})
	s.cleanPreWindow = make(chan struct{})
	s.fieldCallsLen = len(s.fieldCalls)
	if s.concurrency == 0 {
		s.concurrency = 1
	}

	s.corpus = sync.Map{}
	s.corpusIndexes = []string{EmptyGroupKey}

	s.innerCache = make(chan *WindowCache, s.concurrency)
	s.innerRes = make(chan error, s.concurrency)

	s.ptIds = make([]*uint32, maxWindowNum)
	s.shardIds = make([]*uint64, maxWindowNum)
	for i := 0; i < maxWindowNum; i++ {
		var pt uint32
		var shard uint64
		s.ptIds[i] = &pt
		s.shardIds[i] = &shard
	}
	return nil
}

func (s *TagTask) buildFieldCalls() error {
	for c := range s.fieldCalls {
		switch s.fieldCalls[c].call {
		case "min":
			s.fieldCalls[c].tagFunc = atomic2.CompareAndSwapMinFloat64
		case "max":
			s.fieldCalls[c].tagFunc = atomic2.CompareAndSwapMaxFloat64
		case "sum":
			s.fieldCalls[c].tagFunc = atomic2.AddFloat64
		case "count":
			s.fieldCalls[c].tagFunc = atomic2.AddFloat64
		default:
			return fmt.Errorf("not support stream func %v", s.fieldCalls[c].call)
		}
	}
	return nil
}

// cycle flush data form cache, period is group time
// TODO support stream not contain group time
func (s *TagTask) cycleFlush() {
	var err error
	defer func() {
		if r := recover(); r != nil {
			err := errno.NewError(errno.RecoverPanic, r)
			s.Logger.Error(err.Error())
		}

		s.err = err
	}()
	reset := false
	now := time.Now()
	next := now.Truncate(s.window).Add(s.window).Add(s.maxDelay)
	ticker := time.NewTicker(next.Sub(now))
	for {
		select {
		case <-ticker.C:
			if !reset {
				reset = true
				ticker.Reset(s.window)
				continue
			}
			err := s.flush()
			if err != nil {
				s.Logger.Error("stream flush error", zap.Error(err))
			}
		case <-s.abort:
			return
		}
	}
}

// consume data from inner cache, inner cache size equal to concurrency
// TODO share calculate goroutine with other stream task
func (s *TagTask) parallelCalculate() {
	for i := 0; i < s.concurrency; i++ {
		go func() {
			for {
				select {
				case cache := <-s.innerCache:
					err := s.calculate(cache)
					if err != nil {
						s.Logger.Error("calculate error", zap.String("window", s.name), zap.Error(err))
					}
					select {
					case s.innerRes <- err:
					default:
						panic(fmt.Sprintf("innerRes is full, size %v", len(s.innerRes)))
					}
				case <-s.abort:
					return
				}
			}
		}()
	}
}

// clean old window values, set value nil
// TODO clean window unused key
func (s *TagTask) cleanWindow() {
	for {
		select {
		case _, open := <-s.cleanPreWindow:
			if !open {
				return
			}
			t := time.Now()
			s.values.Range(s.walkUpdate)
			s.stats.StatWindowUpdateCost(int64(time.Since(t)))
		case <-s.abort:
			return
		}
	}
}

// consume data from window cache, and update window metadata, calculate cannot parallel with update window
func (s *TagTask) consumeDataAndUpdateMeta() {
	defer func() {
		if r := recover(); r != nil {
			err := errno.NewError(errno.RecoverPanic, r)
			s.Logger.Error(err.Error())
		}
	}()
	for {
		select {
		case _, open := <-s.updateWindow:
			if !open {
				return
			}
			s.start = s.end
			s.end = s.end.Add(s.window)
			atomic2.SetModInt64AndADD(&s.startWindowID, 1, int64(s.windowNum))
			s.stats.Reset()
			s.stats.StatWindowOutMinTime(s.start.UnixNano())
			s.stats.StatWindowOutMaxTime(s.end.UnixNano())
			select {
			case s.cleanPreWindow <- struct{}{}:
				continue
			case <-s.abort:
				return
			}
		case <-s.abort:
			return
		case cache := <-s.cache:
			s.IncreaseChan()
			count := 0
			s.innerCache <- cache
			count++
			if count < s.concurrency {
				loop := true
				for loop {
					select {
					case c := <-s.cache:
						s.IncreaseChan()
						s.innerCache <- c
						count++
						if count >= s.concurrency {
							loop = false
						}
					default:
						// currently no new data to calculate
						loop = false
					}
				}
			}
			for i := 0; i < count; i++ {
				<-s.innerRes
			}
		}
	}
}

func (s *TagTask) walkUpdate(k, vv interface{}) bool {
	offset := atomic2.LoadModInt64AndADD(&s.startWindowID, -1, int64(s.windowNum))
	// window values only store float64 pointer type, no need to check
	v, _ := vv.([]*float64)
	vs := v[int(offset)*s.fieldCallsLen : int(offset)*s.fieldCallsLen+s.fieldCallsLen]
	for i := range vs {
		vs[i] = nil
	}
	return true
}

func (s *TagTask) calculate(cache *WindowCache) error {
	// occur release func
	if cache == nil {
		panic("cannot be here")
	}
	defer func() {
		cache.release()
		cache.rows = nil
		s.windowCachePool.Put(cache)
	}()
	rows := cache.rows
	s.stats.AddWindowIn(int64(len(rows)))
	s.stats.StatWindowStartTime(s.start.UnixNano())
	s.stats.StatWindowEndTime(s.start.UnixNano() + s.maxDuration)
	for i := range rows {
		row := rows[i]
		if row.Timestamp < s.start.UnixNano() || row.Timestamp >= s.start.UnixNano()+s.maxDuration {
			if row.Timestamp >= s.end.UnixNano() {
				atomic2.CompareAndSwapMaxInt64(&s.stats.WindowOutMaxTime, row.Timestamp)
			} else {
				atomic2.CompareAndSwapMinInt64(&s.stats.WindowOutMinTime, row.Timestamp)
			}
			s.stats.AddWindowSkip(1)
			continue
		}
		key := s.generateGroupKeyUint(s.groupKeys, &row)
		vv, exist := s.values.Load(key)
		var vs []*float64
		if !exist {
			vs = make([]*float64, s.fieldCallsLen*s.windowNum)
			s.values.Store(key, vs)
			s.stats.AddWindowGroupKeyCount(1)
		} else {
			vs, _ = vv.([]*float64)
		}
		windowId := int((row.Timestamp-s.start.UnixNano())/s.window.Nanoseconds()+atomic.LoadInt64(&s.startWindowID)) % s.windowNum
		for c := range s.fieldCalls {
			var curVal float64
			// count op, if streamOnly, add value, else add 1
			if s.fieldCalls[c].call == "count" && !row.StreamOnly {
				curVal = 1
			} else {
				for f := range row.Fields {
					if row.Fields[f].Key == s.fieldCalls[c].name || row.Fields[f].Key == s.fieldCalls[c].alias {
						curVal = row.Fields[f].NumValue
						break
					}
				}
			}
			id := s.fieldCallsLen*windowId + c
			if vs[id] == nil {
				var t float64
				if s.fieldCalls[c].call == "min" {
					t = math.MaxFloat64
				} else if s.fieldCalls[c].call == "max" {
					t = -math.MaxFloat64
				}
				atomic2.SetAndSwapPointerFloat64(&vs[id], &t)
			}
			s.fieldCalls[c].tagFunc(vs[id], curVal)
		}
		atomic.SwapUint64(s.shardIds[windowId], cache.shardId)
		atomic.StoreUint32(s.ptIds[windowId], cache.ptId)
		s.stats.AddWindowProcess(1)
	}

	return nil
}

// generateRows generate rows from map cache, prepare data for flush
func (s *TagTask) generateRows() {
	s.offset = int(atomic.LoadInt64(&s.startWindowID)) * s.fieldCallsLen
	s.values.Range(s.buildRow)
}

func (s *TagTask) buildRow(k any, vv any) bool {
	key, _ := k.(string)
	// window values only store float64 pointer type, no need to check
	v, _ := vv.([]*float64)
	if s.validNum >= len(s.rows) {
		s.rows = append(s.rows, influx.Row{Name: s.info.Name})
	}
	s.rows[s.validNum].ReFill()
	// reuse rows body
	fields := &s.rows[s.validNum].Fields
	// once make, reuse every flush
	if fields.Len() < len(s.fieldCalls) {
		*fields = make([]influx.Field, len(s.fieldCalls))
		for i := 0; i < fields.Len(); i++ {
			(*fields)[i] = influx.Field{
				Key:  s.fieldCalls[i].alias,
				Type: s.fieldCalls[i].outFieldType,
			}
		}
	}
	validNum := 0
	for i := range s.fieldCalls {
		if v[s.offset+i] == nil {
			continue
		}
		(*fields)[validNum].NumValue = atomic2.LoadFloat64(v[s.offset+i])
		validNum++
	}
	if validNum == 0 {
		return true
	}
	*fields = (*fields)[:validNum]
	tags := &s.rows[s.validNum].Tags
	if key == EmptyGroupKey {
		if len(s.groupKeys) != 0 {
			s.Logger.Error("buildRow fail", zap.Error(fmt.Errorf("cannot occur this groupkeys %v key %v", key, s.groupKeys)))
			return true
		}
	} else {
		var err error
		values := strings.Split(key, config.StreamGroupValueStrSeparator)
		if len(values) != len(s.groupKeys) {
			s.Logger.Error("buildRow fail", zap.Error(fmt.Errorf("cannot occur this values %v len %v groupkeys %v key %v", values, len(values), s.groupKeys, key)))
			return true
		}
		validNum = 0
		// once make, reuse every flush
		if tags.Len() < len(s.groupKeys) {
			*tags = make([]influx.Tag, len(s.groupKeys))
			for i := 0; i < tags.Len(); i++ {
				(*tags)[i] = influx.Tag{}
			}
		}
		for i := range s.groupKeys {
			value := values[i]
			value, err = s.unCompressDictKey(value)
			if err != nil {
				s.Logger.Error("unCompressDictKey fail", zap.Error(err))
				return true
			}
			// empty value, skip
			if value == EmptyTagValue {
				continue
			}
			(*tags)[validNum].Value = value
			(*tags)[validNum].Key = s.groupKeys[i]
			validNum++
		}
		*tags = (*tags)[:validNum]
	}
	s.rows[s.validNum].Timestamp = s.start.UnixNano()
	s.indexKeyPool = s.rows[s.validNum].UnmarshalIndexKeys(s.indexKeyPool)
	s.validNum++
	return true
}

// corpusIndexes array not need lock
func (s *TagTask) unCompressDictKey(key string) (string, error) {
	if key == EmptyTagValue {
		return EmptyTagValue, nil
	}
	intV, err := strconv.Atoi(key)
	if err != nil {
		s.Logger.Error(fmt.Sprintf("invalid corpus key %v", key))
		return "", err
	}
	if len(s.corpusIndexes) < intV-1 {
		s.Logger.Error(fmt.Sprintf("corpusIndexes len is less than %v", intV-1))
		return "", err
	}
	return s.corpusIndexes[intV], nil
}

// UnCompressDictKeyUint corpusIndexes array not need lock
func (s *TagTask) UnCompressDictKeyUint(key uint64) (string, error) {
	if uint64(len(s.corpusIndexes)) < key-1 {
		return "", fmt.Errorf("corpusIndexes len is less than %v", key-1)
	}
	return s.corpusIndexes[key], nil
}

// no lock to compress dict key, compress string to int64
func (s *TagTask) compressDictKeyUint(key string) (uint64, error) {
	vv, ok := s.corpus.Load(key)
	if ok {
		index, _ := vv.(uint64)
		if uint64(len(s.corpusIndexes))+1 < index {
			return 0, errors.New("compressDict index out of range")
		}
		return index, nil
	}
	index := atomic.AddUint64(&s.corpusIndex, 1)
	for uint64(len(s.corpusIndexes)) <= index {
		s.corpusLock.Lock()
		if uint64(len(s.corpusIndexes)) > index {
			s.corpusLock.Unlock()
			break
		}
		s.corpusIndexes = append(s.corpusIndexes, "")
		s.corpusIndexes = s.corpusIndexes[:cap(s.corpusIndexes)]
		s.corpusLock.Unlock()
	}
	key = stringinterner.InternTagValue(key)
	s.corpusIndexes[index] = key
	s.corpus.Store(key, index)
	// conflict situation, may overwrite kv, but do not difference for uncompress, because corpusIndexes not overwrite
	return index, nil
}

func (s *TagTask) flush() error {
	var err error
	s.Logger.Info("stream start flush")
	t := time.Now()
	s.indexKeyPool = bufferpool.GetPoints()
	defer func() {
		bufferpool.Put(s.indexKeyPool)
		s.indexKeyPool = nil
		s.stats.StatWindowFlushCost(int64(time.Since(t)))
		s.stats.Push()
		select {
		case s.updateWindow <- struct{}{}:
			return
		case <-s.abort:
			return
		}
	}()

	s.generateRows()
	if s.validNum == 0 {
		return err
	}

	// if the number of rows is not greater than the FlushParallelMinRowNum, the rows will be flushed serially.
	if s.validNum <= FlushParallelMinRowNum {
		err = s.WriteRowsToShard(0, s.validNum)
		s.Logger.Info("stream flush over")
		s.rows = s.rows[0:]
		s.validNum = 0
		return err
	}

	// if the number of rows is greater than the FlushParallelMinRowNum, the rows will be flushed in parallel.
	conNum := s.validNum / s.concurrency
	for i := 0; i < s.concurrency; i++ {
		start, end := i*conNum, 0
		if i < s.concurrency-1 {
			end = (i + 1) * conNum
		} else {
			end = s.validNum
		}
		s.flushWG.Add(1)
		_ = s.goPool.Submit(func() {
			err = s.WriteRowsToShard(start, end)
			if err != nil {
				s.Logger.Error("stream flush fail", zap.Error(err))
			}
			s.flushWG.Done()
		})
	}
	s.flushWG.Wait()

	s.Logger.Info("stream flush over")
	s.rows = s.rows[0:]
	s.validNum = 0
	return err
}

func (s *TagTask) WriteRowsToShard(start, end int) error {
	pBuf := bufferpool.GetPoints()
	defer func() {
		bufferpool.PutPoints(pBuf)
	}()

	var err error
	pBuf = append(pBuf[:0], netstorage.PackageTypeFast)
	// db
	pBuf = append(pBuf, uint8(len(s.des.Database)))
	pBuf = append(pBuf, s.des.Database...)
	// rp
	pBuf = append(pBuf, uint8(len(s.des.RetentionPolicy)))
	pBuf = append(pBuf, s.des.RetentionPolicy...)
	// ptid

	pBuf = numenc.MarshalUint32(pBuf, atomic.LoadUint32(s.ptIds[atomic.LoadInt64(&s.startWindowID)]))
	// shardId
	pBuf = numenc.MarshalUint64(pBuf, atomic.LoadUint64(s.shardIds[atomic.LoadInt64(&s.startWindowID)]))
	// rows
	pBuf, err = influx.FastMarshalMultiRows(pBuf, s.rows[start:end])

	if err != nil {
		s.Logger.Error("stream FastMarshalMultiRows fail", zap.Error(err))
		return err
	}

	err = s.store.WriteRows(s.des.Database, s.des.RetentionPolicy,
		atomic.LoadUint32(s.ptIds[atomic.LoadInt64(&s.startWindowID)]),
		atomic.LoadUint64(s.shardIds[atomic.LoadInt64(&s.startWindowID)]), s.rows[start:end], pBuf)
	if err != nil {
		s.Logger.Error("stream flush fail", zap.Error(err))
	}
	return nil
}

func (s *TagTask) Drain() {
	for s.bp.Len() != s.bp.Size() {
	}
	for s.Len() != 0 {
	}
}

// generateGroupKeyUint not support fieldIndex
func (s *TagTask) generateGroupKeyUint(keys []string, value *influx.Row) string {
	if len(keys) == 0 {
		return EmptyGroupKey
	}
	builder := s.bp.Get()
	defer func() {
		builder.Reset()
		s.bp.Put(builder)
	}()

	tagIndex := 0
	for i := range keys {
		idx := Search(tagIndex, len(value.Tags), func(j int) bool { return value.Tags[j].Key >= keys[i] })
		if idx < len(value.Tags) && value.Tags[idx].Key == keys[i] {
			v, err := s.compressDictKeyUint(value.Tags[idx].Value)
			if err != nil {
				panic(fmt.Sprintf("CompressDictKey fail : %v", err))
			}
			builder.AppendString(strconv.FormatUint(v, 10))
			if i < len(keys)-1 {
				builder.AppendByte(config.StreamGroupValueSeparator)
			}
			tagIndex = idx + 1
			continue
		} else {
			if i < len(keys)-1 {
				builder.AppendByte(config.StreamGroupValueSeparator)
			}
			tagIndex = idx + 1
			continue
		}
	}
	return builder.NewString()
}

func Search(begin, n int, f func(int) bool) int {
	i, j := begin, n
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		if !f(h) {
			i = h + 1 // preserves f(i-1) == false
		} else {
			j = h // preserves f(j) == true
		}
	}
	return i
}
