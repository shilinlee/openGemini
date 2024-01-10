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

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/openGemini/openGemini/coordinator"
	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/ski"
	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/lib/binaryfilterfunc"
	"github.com/openGemini/openGemini/lib/bucket"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/interruptsignal"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/stringinterner"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/dictpool"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/pingcap/failpoint"
	"go.uber.org/zap"
)

const (
	MaxRetryUpdateOnShardNum    = 4
	minDownSampleConcurrencyNum = 1
	cpuDownSampleRatio          = 16
	CRCLen                      = 4
	BufferSize                  = 1024 * 1024
)

var (
	DownSampleWriteDrop  = true
	downSampleLogSeq     = uint64(time.Now().UnixNano())
	downSampleInorder    = false
	maxDownSampleTaskNum int
)

type Storage interface {
	writeSnapshot(s *shard)
	WriteRowsToTable(s *shard, rows influx.Rows, mw *mstWriteCtx, binaryRows []byte) error
	WriteRows(s *shard, mw *mstWriteCtx) error                                    // line protocol
	WriteCols(s *shard, cols *record.Record, mst string, binaryCols []byte) error // native protocol
	WriteIndex(s *shard, rows *influx.Rows, mw *mstWriteCtx) error
	shouldSnapshot(s *shard) bool
	SetClient(client metaclient.MetaClient)
	SetMstInfo(s *shard, name string, mstInfo *meta.MeasurementInfo)
	SetAccumulateMetaIndex(name string, detachedMetaInfo *immutable.AccumulateMetaIndex)
	ForceFlush(s *shard)
}

type tsstoreImpl struct {
}

func (storage *tsstoreImpl) WriteRows(s *shard, mw *mstWriteCtx) error {
	mmPoints := mw.getMstMap()
	mw.initWriteRowsCtx(s.getLastFlushTime, s.addRowCountsBySid, nil)
	return s.activeTbl.MTable.WriteRows(s.activeTbl, mmPoints, mw.writeRowsCtx)
}

func (storage *tsstoreImpl) WriteRowsToTable(s *shard, rows influx.Rows, mw *mstWriteCtx, binaryRows []byte) error {
	// alloc token
	// Token is released during the snapshot process, the number of tokens needs to be recorded before data is written.
	start := time.Now()
	curSize := calculateMemSize(rows)
	err := nodeMutableLimit.allocResource(curSize, mw.timer)
	atomic.AddInt64(&statistics.PerfStat.WriteGetTokenDurationNs, time.Since(start).Nanoseconds())
	if err != nil {
		s.log.Info("Alloc resource failed, need retry", zap.Int64("current mem size", curSize))
		return err
	}

	// write index
	indexErr := storage.WriteIndex(s, &rows, mw)
	if indexErr != nil && !errno.Equal(indexErr, errno.SeriesLimited) {
		nodeMutableLimit.freeResource(curSize)
		return indexErr
	}

	// write data to mem table and write wal
	err = s.writeRows(mw, binaryRows, curSize)
	if err != nil {
		return err
	}

	return indexErr
}

func (storage *tsstoreImpl) WriteCols(s *shard, cols *record.Record, mst string, binaryCols []byte) error {
	return errors.New("not implement yet")
}

func (storage *tsstoreImpl) WriteIndex(s *shard, rowsPointer *influx.Rows, mw *mstWriteCtx) error {
	rows := *rowsPointer
	mmPoints := mw.getMstMap()
	var err error

	start := time.Now()
	if !sort.IsSorted(&rows) {
		sort.Stable(&rows)
	}
	atomic.AddInt64(&statistics.PerfStat.WriteSortIndexDurationNs, time.Since(start).Nanoseconds())

	var writeIndexRequired bool
	start = time.Now()
	tm := int64(math.MinInt64)
	primaryIndex := s.indexBuilder.GetPrimaryIndex()
	idx, _ := primaryIndex.(*tsi.MergeSetIndex)
	for i := 0; i < len(rows); i++ {
		if s.closed.Closed() {
			return errno.NewError(errno.ErrShardClosed, s.ident.ShardID)
		}
		//skip StreamOnly data
		if rows[i].StreamOnly {
			continue
		}

		ri := cloneRowToDict(mmPoints, mw, &rows[i])
		if ri.Timestamp > tm {
			tm = ri.Timestamp
		}
		if !writeIndexRequired {
			ri.SeriesId, err = idx.GetSeriesIdBySeriesKey(rows[i].IndexKey, util.Str2bytes(rows[i].Name))
			if err != nil {
				return err
			}
			// PrimaryId is equal to SeriesId by default.
			ri.PrimaryId = ri.SeriesId

			if ri.SeriesId == 0 {
				writeIndexRequired = true
			}
		}
		atomic.AddInt64(&statistics.PerfStat.WriteFieldsCount, int64(rows[i].Fields.Len()))
	}

	s.setMaxTime(tm)

	failpoint.Inject("SlowDownCreateIndex", nil)
	if writeIndexRequired {
		if err = s.indexBuilder.CreateIndexIfNotExists(mmPoints, true); err != nil {
			return err
		}
	} else {
		if err = s.indexBuilder.CreateSecondaryIndexIfNotExist(mmPoints); err != nil {
			return err
		}
	}
	atomic.AddInt64(&statistics.PerfStat.WriteIndexDurationNs, time.Since(start).Nanoseconds())
	return nil
}

func (storage *tsstoreImpl) SetClient(client metaclient.MetaClient) {}

func (storage *tsstoreImpl) SetMstInfo(s *shard, name string, mstInfo *meta.MeasurementInfo) {}

func (storage *tsstoreImpl) SetAccumulateMetaIndex(name string, detachedMetaInfo *immutable.AccumulateMetaIndex) {
}

func (storage *tsstoreImpl) shouldSnapshot(s *shard) bool {
	if s.activeTbl == nil || s.snapshotTbl != nil || s.forceFlushing() {
		return false
	}
	return true
}

func (storage *tsstoreImpl) ForceFlush(s *shard) {
	if s.indexBuilder == nil {
		return
	}
	s.enableForceFlush()
	defer s.disableForceFlush()

	s.waitSnapshot()
	s.prepareSnapshot()
	s.storage.writeSnapshot(s)
	s.endSnapshot()
}

func (storage *tsstoreImpl) writeSnapshot(s *shard) {
	s.snapshotLock.Lock()
	if s.activeTbl == nil {
		s.snapshotLock.Unlock()
		return
	}
	walFiles, err := s.wal.Switch()
	if err != nil {
		s.snapshotLock.Unlock()
		panic("wal switch failed")
	}

	s.snapshotTbl = s.activeTbl
	curSize := s.snapshotTbl.GetMemSize()

	s.activeTbl = s.memTablePool.Get(s.engineType)
	s.activeTbl.SetIdx(s.skIdx)
	s.snapshotLock.Unlock()

	start := time.Now()
	s.indexBuilder.Flush()

	s.commitSnapshot(s.snapshotTbl)
	nodeMutableLimit.freeResource(curSize)

	err = s.wal.Remove(walFiles)
	if err != nil {
		panic("wal remove files failed: " + err.Error())
	}

	//This fail point is used in scenarios where "s.snapshotTbl" is not recycled
	failpoint.Inject("snapshot-table-reset-delay", func() {
		time.Sleep(2 * time.Second)
	})

	s.snapshotLock.Lock()
	s.snapshotTbl.UnRef()
	s.snapshotTbl = nil
	s.snapshotLock.Unlock()

	atomic.AddInt64(&statistics.PerfStat.FlushSnapshotDurationNs, time.Since(start).Nanoseconds())
	atomic.AddInt64(&statistics.PerfStat.FlushSnapshotCount, 1)
}

type columnstoreImpl struct {
	mu                  sync.RWMutex
	client              metaclient.MetaClient
	snapshotContainer   []*mutable.MemTable
	snapshotStatus      []int
	flushManager        map[string]mutable.FlushManager // mst -> flush detached or attached
	accumulateMetaIndex *sync.Map                       //mst -> immutable.AccumulateMetaIndex, record metaIndex for detached store
	mstsInfo            *sync.Map                       // map[cpu-001]meta.MeasurementInfo
}

func newColumnstoreImpl(snapshotTblNum int) *columnstoreImpl {
	return &columnstoreImpl{
		snapshotContainer:   make([]*mutable.MemTable, snapshotTblNum),
		snapshotStatus:      make([]int, snapshotTblNum),
		mstsInfo:            &sync.Map{},
		flushManager:        make(map[string]mutable.FlushManager),
		accumulateMetaIndex: &sync.Map{},
	}
}

func (storage *columnstoreImpl) writeSnapshot(s *shard) {
	s.snapshotLock.Lock()
	if s.activeTbl == nil {
		s.snapshotLock.Unlock()
		return
	}
	walFiles, err := s.wal.Switch()
	if err != nil {
		s.snapshotLock.Unlock()
		panic("wal switch failed")
	}

	idx := storage.getFreeSnapShotTbl()
	if idx == -1 {
		s.snapshotLock.Unlock()
		panic("error: there is not free snapShotTbl")
	}
	//set flushManager and accumulateMetaIndex
	s.activeTbl.MTable.SetFlushManagerInfo(storage.flushManager, storage.accumulateMetaIndex)
	storage.snapshotContainer[idx] = s.activeTbl
	storage.snapshotStatus[idx] = 1
	curSize := storage.snapshotContainer[idx].GetMemSize()

	s.activeTbl = s.memTablePool.Get(s.engineType)
	s.activeTbl.SetIdx(s.skIdx)
	s.snapshotLock.Unlock()

	start := time.Now()
	s.indexBuilder.Flush()

	go func(idx int, curSize int64, walFiles []string, start time.Time) {
		s.commitSnapshot(storage.snapshotContainer[idx])
		nodeMutableLimit.freeResource(curSize)
		err = s.wal.Remove(walFiles)
		if err != nil {
			panic("wal remove files failed: " + err.Error())
		}

		//This fail point is used in scenarios where "s.snapshotTbl" is not recycled
		failpoint.Inject("snapshot-table-reset-delay", func() {
			time.Sleep(2 * time.Second)
		})

		s.snapshotLock.Lock()
		storage.snapshotContainer[idx].UnRef()
		storage.snapshotContainer[idx] = nil
		storage.snapshotStatus[idx] = 0
		s.snapshotLock.Unlock()

		atomic.AddInt64(&statistics.PerfStat.FlushSnapshotDurationNs, time.Since(start).Nanoseconds())
		atomic.AddInt64(&statistics.PerfStat.FlushSnapshotCount, 1)
	}(idx, curSize, walFiles, start)
}

func (storage *columnstoreImpl) WriteRows(s *shard, mw *mstWriteCtx) error {
	mmPoints := mw.getMstMap()
	mw.initWriteRowsCtx(s.getLastFlushTime, s.addRowCountsBySid, storage.mstsInfo)
	mw.writeRowsCtx.MsRowCount = s.msRowCount
	return s.activeTbl.MTable.WriteRows(s.activeTbl, mmPoints, mw.writeRowsCtx)
}

func (storage *columnstoreImpl) WriteRowsToTable(s *shard, rows influx.Rows, mw *mstWriteCtx, binaryRows []byte) error {
	var indexErr error
	var indexWg sync.WaitGroup
	indexWg.Add(1)
	err := storage.updateMstMap(s, rows, mw)
	if err != nil {
		return err
	}

	// alloc token
	// Token is released during the snapshot process, the number of tokens needs to be recorded before data is written.
	start := time.Now()
	curSize := calculateMemSize(rows)
	err = nodeMutableLimit.allocResource(curSize, mw.timer)
	atomic.AddInt64(&statistics.PerfStat.WriteGetTokenDurationNs, time.Since(start).Nanoseconds())
	if err != nil {
		s.log.Info("Alloc resource failed, need retry", zap.Int64("current mem size", curSize))
		return err
	}

	go func() {
		writeIndexStart := time.Now()
		indexErr = storage.WriteIndex(s, &rows, mw)
		if indexErr != nil {
			nodeMutableLimit.freeResource(curSize)
		}
		indexWg.Done()
		atomic.AddInt64(&statistics.PerfStat.WriteIndexDurationNs, time.Since(writeIndexStart).Nanoseconds())
	}()
	err = s.writeRows(mw, binaryRows, curSize)
	indexWg.Wait()
	if err != nil {
		return err
	}

	return indexErr
}

func (storage *columnstoreImpl) UpdateMstsInfo(s *shard, msName, db, rp string) error {
	mst := stringinterner.InternSafe(msName)
	_, ok := storage.mstsInfo.Load(mst)
	if !ok {
		mInfo, err := storage.client.GetMeasurementInfoStore(db, rp, influx.GetOriginMstName(mst))
		if err != nil {
			return err
		}
		err = storage.checkMstInfo(mInfo)
		if err != nil {
			return err
		}
		storage.SetMstInfo(s, mst, mInfo)
	}
	return nil
}

func (storage *columnstoreImpl) updateMstMap(s *shard, rows influx.Rows, mw *mstWriteCtx) error {
	mmPoints := mw.getMstMap()
	tm := int64(math.MinInt64)
	for i := 0; i < len(rows); i++ {
		if s.closed.Closed() {
			return errno.NewError(errno.ErrShardClosed, s.ident.ShardID)
		}
		//skip StreamOnly data
		if rows[i].StreamOnly {
			continue
		}

		//update mstsInfo
		err := storage.UpdateMstsInfo(s, rows[i].Name, s.ident.OwnerDb, s.ident.Policy)
		if err != nil {
			return err
		}
		ri := cloneRowToDict(mmPoints, mw, &rows[i])
		if ri.Timestamp > tm {
			tm = ri.Timestamp
		}
		atomic.AddInt64(&statistics.PerfStat.WriteFieldsCount, int64(rows[i].Fields.Len())+int64(rows[i].Tags.Len()))
	}
	s.setMaxTime(tm)
	return nil
}

func (storage *columnstoreImpl) SetMstInfo(s *shard, name string, mstInfo *meta.MeasurementInfo) {
	storage.mstsInfo.Store(name, mstInfo)
	s.immTables.SetMstInfo(name, mstInfo)
}

func (storage *columnstoreImpl) SetAccumulateMetaIndex(name string, aMetaIndex *immutable.AccumulateMetaIndex) {
	storage.accumulateMetaIndex.Store(name, aMetaIndex)
}

func (storage *columnstoreImpl) shouldSnapshot(s *shard) bool {
	if s.activeTbl == nil || !storage.isSnapShotTblFree() || s.forceFlushing() {
		return false
	}
	return true
}

func (storage *columnstoreImpl) isSnapShotTblFree() bool {
	for i := range storage.snapshotContainer {
		if storage.snapshotContainer[i] == nil {
			return true
		}
	}
	return false
}

func (storage *columnstoreImpl) ForceFlush(s *shard) {
	if s.indexBuilder == nil {
		return
	}
	s.enableForceFlush()
	defer s.disableForceFlush()

	s.waitSnapshot()
	idx := storage.getFreeSnapShotTbl()
	if idx == -1 {
		log.Debug("there is no free snapshot table", zap.Uint64("shard id", s.ident.ShardID))
		return
	}
	s.prepareSnapshot()
	s.storage.writeSnapshot(s)
	s.endSnapshot()
}

func (storage *columnstoreImpl) getFreeSnapShotTbl() int {
	for i := range storage.snapshotStatus {
		if storage.snapshotStatus[i] == 0 {
			return i
		}
	}
	return -1
}

func (storage *columnstoreImpl) WriteCols(s *shard, cols *record.Record, mst string, binaryCols []byte) error {
	if cols == nil {
		return errors.New("write rec can not be nil")
	}
	s.wg.Add(1)
	s.writeWg.Add(1)
	defer func() {
		s.wg.Done()
		s.writeWg.Done()
	}()

	if s.ident.ReadOnly {
		err := errors.New("can not write cols to downSampled shard")
		log.Error("write into shard failed", zap.Error(err))
		if !getDownSampleWriteDrop() {
			return err
		}
		return nil
	}

	mw := getMstWriteRecordCtx(nodeMutableLimit.timeOut, s.engineType)
	defer putMstWriteRecordCtx(mw)

	// alloc token
	start := time.Now()
	curSize := int64(cols.Size())
	err := nodeMutableLimit.allocResource(curSize, mw.timer)
	atomic.AddInt64(&statistics.PerfStat.WriteGetTokenDurationNs, time.Since(start).Nanoseconds())
	if err != nil {
		s.log.Info("Alloc resource failed, need retry", zap.Int64("current mem size", curSize))
		return err
	}

	var indexErr error
	var indexWg sync.WaitGroup
	indexWg.Add(1)

	//update mstsInfo
	err = storage.UpdateMstsInfo(s, mst, s.ident.OwnerDb, s.ident.Policy)
	if err != nil {
		return err
	}

	//write index
	go func() {
		writeIndexStart := time.Now()
		indexErr = storage.WriteIndexForCols(s, cols, mst)
		indexWg.Done()
		atomic.AddInt64(&statistics.PerfStat.WriteIndexDurationNs, time.Since(writeIndexStart).Nanoseconds())
	}()

	// write data and wal
	err = s.writeCols(cols, binaryCols, mst)
	indexWg.Wait()
	if err != nil {
		return err
	}
	s.activeTbl.AddMemSize(curSize)
	return indexErr
}

func (storage *columnstoreImpl) writecols(s *shard, cols *record.Record, mst string) error {
	// update the row count for each mst
	storage.mu.Lock()
	mutable.UpdateMstRowCount(s.msRowCount, mst, int64(cols.RowNums()))
	storage.mu.Unlock()
	return s.activeTbl.MTable.WriteCols(s.activeTbl, cols, storage.mstsInfo, mst)
}

func (storage *columnstoreImpl) WriteIndex(s *shard, rows *influx.Rows, mw *mstWriteCtx) error {
	mmPoints := mw.getMstMap()
	return s.indexBuilder.CreateIndexIfNotExists(mmPoints, false)
}

func (storage *columnstoreImpl) WriteIndexForCols(s *shard, cols *record.Record, mst string) error {
	if s.closed.Closed() {
		return errno.NewError(errno.ErrShardClosed, s.ident.ShardID)
	}
	mst = stringinterner.InternSafe(mst)
	msInfo, ok := mutable.GetMsInfo(mst, storage.mstsInfo)
	if !ok {
		s.log.Info("mstInfo is nil", zap.String("mst name", mst))
		return errors.New("measurement info is not found")
	}
	tagIndex := findTagIndex(cols.Schemas(), msInfo.Schema)

	// write index
	return s.indexBuilder.CreateIndexIfNotExistsByCol(cols, tagIndex, mst)
}

func (storage *columnstoreImpl) SetClient(client metaclient.MetaClient) {
	storage.client = client
}

func (storage *columnstoreImpl) checkMstInfo(mstInfo *meta.MeasurementInfo) error {
	if mstInfo == nil || mstInfo.ColStoreInfo.PrimaryKey == nil || mstInfo.ColStoreInfo.SortKey == nil {
		return errors.New("the key component of mstInfo is nil")
	}
	return nil
}

func findTagIndex(schema record.Schemas, metaSchema map[string]int32) []int {
	var res []int
	for i := range schema {
		if metaSchema[schema[i].Name] == influx.Field_Type_Tag { // according to the meta schema
			res = append(res, i)
		}
	}
	return res
}

type Shard interface {
	// IO interface
	WriteRows(rows []influx.Row, binaryRows []byte) error               // line protocol
	WriteCols(mst string, cols *record.Record, binaryCols []byte) error // native protocol
	ForceFlush()
	WaitWriteFinish()
	CreateLogicalPlan(ctx context.Context, sources influxql.Sources, schema *executor.QuerySchema) (hybridqp.QueryNode, error)
	CreateCursor(ctx context.Context, schema *executor.QuerySchema) ([]comm.KeyCursor, error)
	Scan(span *tracing.Span, schema *executor.QuerySchema, callBack func(num int64) error) (tsi.GroupSeries, int64, error)
	ScanWithSparseIndex(ctx context.Context, schema *executor.QuerySchema, callBack func(num int64) error) (*executor.FileFragments, error)
	GetIndexInfo(schema *executor.QuerySchema) (*executor.AttachedIndexInfo, error)
	RowCount(schema *executor.QuerySchema) (int64, error)
	NewShardKeyIdx(shardType, dataPath string, lockPath *string) error

	// admin
	OpenAndEnable(client metaclient.MetaClient) error
	IsOpened() bool
	Close() error
	ChangeShardTierToWarm()
	DropMeasurement(ctx context.Context, name string) error
	GetSplitPoints(idxes []int64) ([]string, error) // only work for tsstore (depends on sid)

	// get private member
	GetDataPath() string
	GetWalPath() string
	GetDuration() *meta.DurationDescriptor
	GetEngineType() config.EngineType
	GetIdent() *meta.ShardIdentifier
	GetID() uint64
	GetRowCount() uint64
	GetRPName() string
	GetStatistics(buffer []byte) ([]byte, error)
	GetMaxTime() int64
	GetIndexBuilder() *tsi.IndexBuilder                                // only work for tsstore(tsi)
	GetSeriesCount() int                                               // only work for tsstore
	GetTableStore() immutable.TablesStore                              // used by downsample and test
	GetTSSPFiles(mm string, isOrder bool) (*immutable.TSSPFiles, bool) // used by downsample and test
	GetTier() uint64
	IsExpired() bool
	IsTierExpired() bool

	// downsample, only work for tsstore
	CanDoDownSample() bool
	DisableDownSample()
	EnableDownSample()
	GetShardDownSamplePolicy(policy *meta.DownSamplePolicyInfo) *meta.ShardDownSamplePolicyInfo
	IsOutOfOrderFilesExist() bool
	NewDownSampleTask(sdsp *meta.ShardDownSamplePolicyInfo, schema []hybridqp.Catalog, log *zap.Logger)
	SetShardDownSampleLevel(i int)
	SetMstInfo(mstsInfo *meta.MeasurementInfo)
	StartDownSample(taskID uint64, level int, sdsp *meta.ShardDownSamplePolicyInfo, meta interface {
		UpdateShardDownSampleInfo(Ident *meta.ShardIdentifier) error
	}) error
	UpdateDownSampleOnShard(id uint64, level int)
	UpdateShardReadOnly(meta interface {
		UpdateShardDownSampleInfo(Ident *meta.ShardIdentifier) error
	}) error

	// compaction && merge, only work for tsstore
	Compact() error
	DisableCompAndMerge()
	EnableCompAndMerge()
	SetLockPath(lock *string)
}

type shard struct {
	mu        sync.RWMutex
	wg        sync.WaitGroup
	writeWg   sync.WaitGroup
	opId      uint64
	closed    *interruptsignal.InterruptSignal
	dataPath  string
	filesPath string
	walPath   string
	lock      *string
	ident     *meta.ShardIdentifier

	opened bool // is shard opened

	cacheClosed        int32
	isAsyncReplayWal   bool               // async replay wal switch
	cancelLock         sync.RWMutex       // lock for cancelFn
	cancelFn           context.CancelFunc // to cancel wal replay
	loadWalDone        bool               // is replay wal done
	wal                *WAL               // for cases: 1. write 2. replay
	snapshotLock       sync.RWMutex
	memDataReadEnabled bool
	activeTbl          *mutable.MemTable
	snapshotTbl        *mutable.MemTable
	snapshotWg         sync.WaitGroup
	immTables          immutable.TablesStore
	indexBuilder       *tsi.IndexBuilder
	skIdx              *ski.ShardKeyIndex
	pkIndexReader      sparseindex.PKIndexReader
	skIndexReader      sparseindex.SKIndexReader
	rowCount           int64
	tmLock             sync.RWMutex
	maxTime            int64
	startTime          time.Time
	endTime            time.Time
	durationInfo       *meta.DurationDescriptor
	log                *Log.Logger
	droppedMst         sync.Map
	msRowCount         *sync.Map

	tier uint64

	lastWriteTime uint64

	writeColdDuration time.Duration

	forceFlush  bool
	forceChan   chan struct{}
	defaultTags map[string]string
	fileStat    *statistics.FileStatistics

	shardDownSampleTaskInfo *shardDownSampleTaskInfo

	stopDownSample chan struct{}
	dswg           sync.WaitGroup
	downSampleEn   int32

	engineType config.EngineType
	storage    Storage

	seriesLimit uint64
	//lint:ignore U1000 use for replication feature
	summary *summaryInfo

	memTablePool *mutable.MemTablePool
}

type shardDownSampleTaskInfo struct {
	sdsp   *meta.ShardDownSamplePolicyInfo
	schema []hybridqp.Catalog
	log    *zap.Logger
}

type nodeMemBucket struct {
	once      sync.Once
	memBucket bucket.ResourceBucket
	timeOut   time.Duration
}

var nodeMutableLimit nodeMemBucket

func (nodeLimit *nodeMemBucket) initNodeMemBucket(timeOut time.Duration, memThreshold int64) {
	nodeLimit.once.Do(func() {
		log.Info("New node mem limit bucket", zap.Int64("node mutable size limit", memThreshold),
			zap.Duration("max write hang duration", timeOut))
		nodeLimit.memBucket = bucket.NewInt64Bucket(timeOut, memThreshold, false)
		nodeLimit.timeOut = timeOut
	})
}

func (nodeLimit *nodeMemBucket) allocResource(r int64, timer *time.Timer) error {
	return nodeLimit.memBucket.GetResDetected(r, timer)
}

func (nodeLimit *nodeMemBucket) freeResource(r int64) {
	nodeLimit.memBucket.ReleaseResource(r)
}

func getWalPartitionNum() int {
	cpuNum := cpu.GetCpuNum()
	if cpuNum <= 16 {
		return cpuNum
	}
	return 16
}

func initMaxDownSampleParallelism(parallelism int) {
	if parallelism <= 0 {
		parallelism = hybridqp.MaxInt(cpu.GetCpuNum()/cpuDownSampleRatio, minDownSampleConcurrencyNum)
	}
	maxDownSampleTaskNum = parallelism
}

func setDownSampleWriteDrop(enabled bool) {
	DownSampleWriteDrop = enabled
}

func getDownSampleWriteDrop() bool {
	return DownSampleWriteDrop
}

func NewShard(dataPath, walPath string, lockPath *string, ident *meta.ShardIdentifier, durationInfo *meta.DurationDescriptor, tr *meta.TimeRangeInfo,
	options netstorage.EngineOptions, engineType config.EngineType) *shard {
	db, rp := decodeShardPath(dataPath)
	filePath := immutable.GetDir(engineType, dataPath)

	lock := fileops.FileLockOption(*lockPath)
	err := fileops.MkdirAll(filePath, 0750, lock)
	if err != nil {
		panic(err)
	}
	err = fileops.MkdirAll(walPath, 0750, lock)
	if err != nil {
		panic(err)
	}

	nodeMutableLimit.initNodeMemBucket(options.MaxWriteHangTime, options.NodeMutableSizeLimit)

	s := &shard{
		closed:             interruptsignal.NewInterruptSignal(),
		dataPath:           dataPath,
		walPath:            walPath,
		filesPath:          filePath,
		lock:               lockPath,
		ident:              ident,
		isAsyncReplayWal:   options.WalReplayAsync,
		wal:                NewWAL(walPath, lockPath, ident.ShardID, options.WalSyncInterval, options.WalEnabled, options.WalReplayParallel, getWalPartitionNum(), options.WalReplayBatchSize),
		activeTbl:          mutable.NewMemTable(engineType),
		memDataReadEnabled: options.MemDataReadEnabled,
		maxTime:            0,
		lastWriteTime:      fasttime.UnixTimestamp(),
		startTime:          tr.StartTime,
		endTime:            tr.EndTime,
		writeColdDuration:  options.WriteColdDuration,
		forceChan:          make(chan struct{}, 1),
		defaultTags: map[string]string{
			"id":              fmt.Sprintf("%d", ident.ShardID),
			"database":        db,
			"retentionPolicy": rp,
		},
		fileStat:       statistics.NewFileStatistics(),
		stopDownSample: make(chan struct{}),
		downSampleEn:   0,
		droppedMst:     sync.Map{},
		msRowCount:     &sync.Map{},
		engineType:     engineType,
		seriesLimit:    uint64(options.MaxSeriesPerDatabase),
		memTablePool:   mutable.NewMemTablePoolManager().Alloc(db + "/" + rp),
	}
	var conf *immutable.Config
	switch engineType {
	case config.TSSTORE:
		s.storage = &tsstoreImpl{}
		conf = immutable.GetTsStoreConfig()
	case config.COLUMNSTORE:
		conf = immutable.GetColStoreConfig()
		s.storage = newColumnstoreImpl(conf.SnapshotTblNum)
	default:
		return nil
	}
	setDownSampleWriteDrop(options.DownSampleWriteDrop)
	initMaxDownSampleParallelism(options.MaxDownSampleTaskConcurrency)

	s.log = Log.NewLogger(errno.ModuleShard)
	s.durationInfo = durationInfo
	tier := s.GetTier()
	expired := s.IsTierExpired()
	s.tier = tier
	if expired {
		if tier == util.Hot {
			s.tier = util.Warm
		} else {
			s.tier = util.Cold
		}
	}
	s.immTables = immutable.NewTableStore(filePath, s.lock, &s.tier, options.CompactRecovery, conf)
	s.immTables.SetAddFunc(s.addRowCounts)
	s.immTables.SetImmTableType(s.engineType)
	return s
}

func (s *shard) writeCols(cols *record.Record, binaryCols []byte, mst string) error {
	s.snapshotLock.RLock()
	defer s.snapshotLock.RUnlock()
	// write data to mem table
	// Token is released during the snapshot process, the number of tokens needs to be recorded before data is written.
	start := time.Now()
	failpoint.Inject("SlowDownActiveTblWrite", nil)
	err := s.storage.(*columnstoreImpl).writecols(s, cols, mst)
	if err != nil {
		log.Error("write cols rec to memory table fail", zap.Uint64("shard", s.ident.ShardID), zap.Error(err))
		return err
	}
	atomic.AddInt64(&statistics.PerfStat.WriteRowsDurationNs, time.Since(start).Nanoseconds())

	// write wal
	start = time.Now()
	failpoint.Inject("SlowDownWalWrite", nil)
	wr := &walRecord{binary: binaryCols, writeWalType: WriteWalArrowFlight}
	if err = s.wal.Write(wr); err != nil {
		log.Error("write cols rec to wal fail", zap.Uint64("shard", s.ident.ShardID), zap.Error(err))
		return err
	}
	atomic.AddInt64(&statistics.PerfStat.WriteWalDurationNs, time.Since(start).Nanoseconds())
	return nil
}

func (s *shard) initSeriesLimiter(limit uint64) {
	if limit == 0 || s.indexBuilder == nil {
		return
	}

	s.indexBuilder.SetSeriesLimiter(func() error {
		if limit > s.immTables.SeriesTotal() {
			return nil
		}
		return errno.NewError(errno.SeriesLimited, s.ident.OwnerDb, limit, s.immTables.SeriesTotal())
	})
}

func (s *shard) NewShardKeyIdx(shardType, dataPath string, lockPath *string) error {
	if shardType != influxql.RANGE {
		return nil
	}
	skToSidIdx, err := ski.NewShardKeyIndex(dataPath, lockPath)
	if err != nil {
		return err
	}
	s.skIdx = skToSidIdx
	s.activeTbl.SetIdx(skToSidIdx)
	return nil
}

func (s *shard) SetWriteColdDuration(duration time.Duration) {
	s.writeColdDuration = duration
}

func (s *shard) isClosing() bool {
	return atomic.LoadInt32(&s.cacheClosed) > 0
}

func (s *shard) WriteRows(rows []influx.Row, binaryRows []byte) error {
	if s.isClosing() {
		return errno.NewError(errno.ErrShardClosed, s.ident.ShardID)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	atomic.StoreUint64(&s.lastWriteTime, fasttime.UnixTimestamp())
	if err := s.writeRowsToTable(rows, binaryRows); err != nil {
		log.Error("write buffer failed", zap.Error(err))
		atomic.AddInt64(&statistics.PerfStat.WriteReqErrors, 1)
		return err
	}

	atomic.AddInt64(&statistics.PerfStat.WriteRowsBatch, 1)
	atomic.AddInt64(&statistics.PerfStat.WriteRowsCount, int64(len(rows)))
	return nil
}

// write data to mem table and write wal
func (s *shard) writeRows(mw *mstWriteCtx, binaryRows []byte, curSize int64) error {
	s.snapshotLock.RLock()
	// write data to mem table
	start := time.Now()
	failpoint.Inject("SlowDownActiveTblWrite", nil)
	err := s.storage.WriteRows(s, mw)
	if err != nil {
		s.activeTbl.AddMemSize(curSize)
		s.snapshotLock.RUnlock()
		log.Error("write rows to memory table fail", zap.Uint64("shard", s.ident.ShardID), zap.Error(err))
		return err
	}
	s.activeTbl.AddMemSize(curSize)
	atomic.AddInt64(&statistics.PerfStat.WriteRowsDurationNs, time.Since(start).Nanoseconds())

	// write wal
	start = time.Now()
	failpoint.Inject("SlowDownWalWrite", nil)
	wr := &walRecord{binary: binaryRows, writeWalType: WriteWalLineProtocol}
	if err = s.wal.Write(wr); err != nil {
		s.snapshotLock.RUnlock()
		log.Error("write rows to wal fail", zap.Uint64("shard", s.ident.ShardID), zap.Error(err))
		return err
	}
	atomic.AddInt64(&statistics.PerfStat.WriteWalDurationNs, time.Since(start).Nanoseconds())

	s.snapshotLock.RUnlock()
	return nil
}

func (s *shard) WriteCols(mst string, cols *record.Record, binaryCols []byte) error {
	if s.isClosing() {
		return errno.NewError(errno.ErrShardClosed, s.ident.ShardID)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	atomic.StoreUint64(&s.lastWriteTime, fasttime.UnixTimestamp())
	if err := s.storage.WriteCols(s, cols, mst, binaryCols); err != nil {
		log.Error("write buffer failed", zap.Error(err))
		atomic.AddInt64(&statistics.PerfStat.WriteReqErrors, 1)
		return err
	}

	s.addRowCounts(int64(cols.RowNums()))
	atomic.AddInt64(&statistics.PerfStat.WriteRowsBatch, 1)
	atomic.AddInt64(&statistics.PerfStat.WriteRowsCount, int64(cols.RowNums()))
	atomic.AddInt64(&statistics.PerfStat.WriteFieldsCount, int64(cols.RowNums()*cols.ColNums()))
	return nil
}

func (s *shard) WaitWriteFinish() {
	s.writeWg.Wait()
}

func (s *shard) shouldSnapshot() bool {
	s.snapshotLock.RLock()
	defer s.snapshotLock.RUnlock()

	if !s.storage.shouldSnapshot(s) {
		return false
	}

	if s.activeTbl != nil && s.activeTbl.GetMemSize() > 0 {
		if s.activeTbl.NeedFlush() {
			s.prepareSnapshot()
			return true
		}

		// check time
		if fasttime.UnixTimestamp() >= (atomic.LoadUint64(&s.lastWriteTime) + uint64(s.writeColdDuration.Seconds())) {
			s.prepareSnapshot()
			return true
		}
	}

	return false
}

func (s *shard) DisableCompAndMerge() {
	s.immTables.DisableCompAndMerge()
}

func (s *shard) EnableCompAndMerge() {
	s.immTables.EnableCompAndMerge()
}

func (s *shard) isDownsampled() bool {
	return s.ident.DownSampleLevel != 0
}

func (s *shard) Compact() error {
	if s.isDownsampled() {
		return nil
	}

	id := s.GetID()
	select {
	case <-s.closed.Signal():
		log.Info("closed", zap.Uint64("shardId", id))
		return nil
	default:
		var rule []uint16
		switch s.engineType {
		case config.COLUMNSTORE:
			rule = immutable.LevelCompactRuleForCs
			if !immutable.GetColStoreConfig().GetCompactionEnabled() {
				s.immTables.CompactionDisable()
			}
		case config.TSSTORE:
			rule = immutable.LevelCompactRule
		}
		if !s.immTables.CompactionEnabled() {
			return nil
		}
		nowTime := fasttime.UnixTimestamp()
		lastWrite := s.LastWriteTime()
		d := nowTime - lastWrite
		if d >= atomic.LoadUint64(&fullCompColdDuration) {
			if err := s.immTables.FullCompact(id); err != nil {
				log.Error("full compact error", zap.Uint64("shid", id), zap.Error(err))
			}
			return nil
		}
		for _, level := range rule {
			if err := s.immTables.LevelCompact(level, id); err != nil {
				log.Error("level compact error", zap.Uint64("shid", id), zap.Error(err))
				continue
			}
		}
	}
	return nil
}

func (s *shard) Snapshot() {
	timer := time.NewTicker(time.Millisecond * 100)
	defer func() {
		s.wg.Done()
		timer.Stop()
	}()
	for {
		select {
		case <-s.closed.Signal():
			return
		case <-timer.C:
			if !s.shouldSnapshot() {
				continue
			}
			s.storage.writeSnapshot(s)
			s.endSnapshot()
		}
	}
}

type mstWriteCtx struct {
	rowsPool     sync.Pool
	mstMap       dictpool.Dict
	timer        *time.Timer
	writeRowsCtx mutable.WriteRowsCtx
	engineType   config.EngineType
}

func (mw *mstWriteCtx) getMstMap() *dictpool.Dict {
	return &mw.mstMap
}

func (mw *mstWriteCtx) getRowsPool() *[]influx.Row {
	v := mw.rowsPool.Get()
	if v == nil {
		return &[]influx.Row{}
	}
	rp := v.(*[]influx.Row)

	return rp
}

func (mw *mstWriteCtx) initWriteRowsCtx(getLastFlushTime func(msName string, sid uint64) int64, addRowCountsBySid func(msName string, sid uint64, rowCounts int64),
	mstsInfo *sync.Map) {
	mw.writeRowsCtx.GetLastFlushTime = getLastFlushTime
	mw.writeRowsCtx.AddRowCountsBySid = addRowCountsBySid
	mw.writeRowsCtx.MstsInfo = mstsInfo
}

func (mw *mstWriteCtx) putRowsPool(rp *[]influx.Row) {
	for i := range *rp {
		(*rp)[i].Reset()
	}
	*rp = (*rp)[:0]
	mw.rowsPool.Put(rp)
}

func (mw *mstWriteCtx) Reset() {
	mw.mstMap.Reset()
}

var mstWriteCtxPool sync.Pool

func getMstWriteCtx(d time.Duration, engineType config.EngineType) *mstWriteCtx {
	v := mstWriteCtxPool.Get()
	if v == nil {
		return &mstWriteCtx{
			timer:      time.NewTimer(d),
			engineType: engineType,
		}
	}
	ctx := v.(*mstWriteCtx)
	ctx.engineType = engineType
	ctx.timer.Reset(d)
	return ctx
}

func putMstWriteCtx(mw *mstWriteCtx) {
	for _, mapp := range mw.mstMap.D {
		rows, ok := mapp.Value.(*[]influx.Row)
		if !ok {
			panic("can't map mmPoints")
		}
		mw.putRowsPool(rows)
	}

	if !mw.timer.Stop() {
		select {
		case <-mw.timer.C:
		default:
		}
	}
	mw.Reset()
	mstWriteCtxPool.Put(mw)
}

type mstWriteRecordCtx struct {
	timer      *time.Timer
	engineType config.EngineType
}

var mstWriteRecordCtxPool sync.Pool

func getMstWriteRecordCtx(d time.Duration, engineType config.EngineType) *mstWriteRecordCtx {
	v := mstWriteRecordCtxPool.Get()
	if v == nil {
		return &mstWriteRecordCtx{
			timer:      time.NewTimer(d),
			engineType: engineType,
		}
	}
	ctx, ok := v.(*mstWriteRecordCtx)
	if !ok {
		log.Error("wrong type switch for mstWriteRecordCtx")
	}
	ctx.engineType = engineType
	ctx.timer.Reset(d)
	return ctx
}

func putMstWriteRecordCtx(mw *mstWriteRecordCtx) {
	if !mw.timer.Stop() {
		select {
		case <-mw.timer.C:
		default:
		}
	}
	mstWriteRecordCtxPool.Put(mw)
}

func (s *shard) getLastFlushTime(msName string, sid uint64) int64 {
	tm := s.immTables.GetLastFlushTimeBySid(msName, sid)
	if tm == math.MaxInt64 || s.snapshotTbl == nil {
		return tm
	}

	snapshotTm := s.snapshotTbl.GetMaxTimeBySidNoLock(msName, sid)
	if snapshotTm > tm {
		tm = snapshotTm
	}

	return tm
}

func (s *shard) addRowCountsBySid(msName string, sid uint64, rowCounts int64) {
	s.immTables.AddRowCountsBySid(msName, sid, rowCounts)
}

func (s *shard) getRowCountsBySid(msName string, sid uint64) (int64, error) {
	return s.immTables.GetRowCountsBySid(msName, sid)
}

func (s *shard) addRowCounts(rowCounts int64) {
	atomic.AddInt64(&s.rowCount, rowCounts)
}

func calculateMemSize(rows influx.Rows) int64 {
	var memCost int64
	for i := range rows {
		// calculate tag mem cost, sid is 8 bytes
		memCost += int64(util.Uint64SizeBytes * len(rows[i].Tags))

		// calculate field mem cost
		for j := 0; j < len(rows[i].Fields); j++ {
			memCost += int64(len(rows[i].Fields[j].Key))
			if rows[i].Fields[j].Type == influx.Field_Type_Float {
				memCost += int64(util.Float64SizeBytes)
			} else if rows[i].Fields[j].Type == influx.Field_Type_String {
				memCost += int64(len(rows[i].Fields[j].StrValue))
			} else if rows[i].Fields[j].Type == influx.Field_Type_Boolean {
				memCost += int64(util.BooleanSizeBytes)
			} else if rows[i].Fields[j].Type == influx.Field_Type_Int {
				memCost += int64(util.Uint64SizeBytes)
			}
		}
	}
	return memCost
}

func cloneRowToDict(mmPoints *dictpool.Dict, mw *mstWriteCtx, row *influx.Row) *influx.Row {
	if !mmPoints.Has(row.Name) {
		rp := mw.getRowsPool()
		mmPoints.Set(row.Name, rp)
	}
	rowsPool := mmPoints.Get(row.Name)
	rp, _ := rowsPool.(*[]influx.Row)

	if cap(*rp) > len(*rp) {
		*rp = (*rp)[:len(*rp)+1]
	} else {
		*rp = append(*rp, influx.Row{})
	}
	ri := &(*rp)[len(*rp)-1]
	ri.Clone(row)
	return ri
}

func (s *shard) writeRowsToTable(rows influx.Rows, binaryRows []byte) error {
	s.wg.Add(1)
	s.writeWg.Add(1)
	defer s.wg.Done()
	defer s.writeWg.Done()
	var err error

	if s.ident.ReadOnly {
		err = errors.New("can not write rows to downSampled shard")
		log.Error("write into shard failed", zap.Error(err))
		if !getDownSampleWriteDrop() {
			return err
		}
		return nil
	}

	mw := getMstWriteCtx(nodeMutableLimit.timeOut, s.engineType)
	defer putMstWriteCtx(mw)

	// write data、wal and index
	err = s.storage.WriteRowsToTable(s, rows, mw, binaryRows)
	s.addRowCounts(int64(len(rows)))
	return err
}

func (s *shard) enableForceFlush() {
	s.forceChan <- struct{}{}
	s.snapshotLock.Lock()
	s.forceFlush = true
	s.snapshotLock.Unlock()
}

func (s *shard) disableForceFlush() {
	s.snapshotLock.Lock()
	s.forceFlush = false
	s.snapshotLock.Unlock()
	<-s.forceChan
}

func (s *shard) forceFlushing() bool {
	return s.forceFlush
}

func (s *shard) ForceFlush() {
	s.storage.ForceFlush(s)
}

func (s *shard) commitSnapshot(snapshot *mutable.MemTable) {
	snapshot.ApplyConcurrency(func(msName string) {
		// do not flush measurement that is deleting
		if s.checkMstDeleting(msName) {
			return
		}
		start := time.Now()
		snapshot.MTable.FlushChunks(snapshot, s.filesPath, msName, s.lock, s.immTables)

		// store the row count of each measurement.
		if rowCount, ok := s.msRowCount.Load(msName); ok {
			if err := mutable.StoreMstRowCount(path.Join(s.dataPath, immutable.ColumnStoreDirName, msName, immutable.CountBinFile), int(*(rowCount.(*int64)))); err != nil {
				s.log.Error(fmt.Sprintf("shard: %s, mst: %s, flush row count failed", s.dataPath, msName))
			}
		}
		atomic.AddInt64(&statistics.PerfStat.SnapshotFlushChunksNs, time.Since(start).Nanoseconds())
	})
}

func (s *shard) prepareSnapshot() {
	s.snapshotWg.Add(1)
}

func (s *shard) endSnapshot() {
	s.snapshotWg.Done()
}

func (s *shard) waitSnapshot() {
	s.snapshotWg.Wait()
}

func (s *shard) GetMaxTime() int64 {
	s.tmLock.RLock()
	tm := s.maxTime
	s.tmLock.RUnlock()
	return tm
}

func (s *shard) GetSeriesCount() int {
	if s.skIdx == nil {
		return 0
	}
	return s.skIdx.GetShardSeriesCount()
}

func (s *shard) GetRowCount() uint64 {
	return uint64(atomic.LoadInt64(&s.rowCount))
}

func (s *shard) GetSplitPoints(idxes []int64) ([]string, error) {
	if s.isClosing() {
		return nil, errno.NewError(errno.ErrShardClosed, s.ident.ShardID)
	}
	return s.getSplitPointsByRowCount(idxes)
}

func (s *shard) getSplitPointsByRowCount(idxes []int64) ([]string, error) {
	return s.skIdx.GetSplitPointsByRowCount(idxes, func(name string, sid uint64) (int64, error) {
		return s.getRowCountsBySid(name, sid)
	})
}

func (s *shard) GetIndexBuilder() *tsi.IndexBuilder {
	if s.isClosing() {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.indexBuilder
}

func (s *shard) Close() error {
	// prevent multi goroutines close shard the same time
	if atomic.AddInt32(&s.cacheClosed, 1) != 1 {
		return errno.NewError(errno.ErrShardClosed, s.ident.ShardID)
	}

	s.DisableDownSample()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.indexBuilder != nil {
		compWorker.UnregisterShard(s.ident.ShardID)
	}

	if s.skIdx != nil {
		if err := s.skIdx.Close(); err != nil {
			return err
		}
	}

	s.closed.Close()

	log.Info("start close shard...", zap.Uint64("id", s.ident.ShardID))
	s.cancelWalReplay()
	if err := s.wal.Close(); err != nil {
		log.Error("close wal fail", zap.Uint64("id", s.ident.ShardID), zap.Error(err))
		return err
	}

	// release mem table resource
	s.snapshotLock.Lock()
	curMemSize := int64(0)
	if s.activeTbl != nil {
		curMemSize = s.activeTbl.GetMemSize()
		s.activeTbl = nil
	}
	s.snapshotLock.Unlock()
	nodeMutableLimit.freeResource(curMemSize)

	log.Info("close immutables", zap.Uint64("id", s.ident.ShardID))
	if err := s.immTables.Close(); err != nil {
		log.Error("close table store fail", zap.Uint64("id", s.ident.ShardID), zap.Error(err))
		return err
	}

	log.Info("success close immutables", zap.Uint64("id", s.ident.ShardID))

	s.wg.Wait()
	return nil
}

func (s *shard) setMaxTime(tm int64) {
	s.tmLock.Lock()
	if tm > s.maxTime {
		s.maxTime = tm
	}
	s.tmLock.Unlock()
}

func (s *shard) writeWalBuffer(binary []byte, rowsCtx *walRowsObjects, writeWalType WalRecordType) error {
	switch writeWalType {
	case WriteWalLineProtocol:
		return s.writeWalForLineProtocol(rowsCtx)
	case WriteWalArrowFlight:
		return s.writeWalForArrowFlight(binary)
	default:
		return errors.New("unKnown write wal type")
	}
}

func (s *shard) writeWalForLineProtocol(rowsCtx *walRowsObjects) error {
	defer func() {
		putWalRowsObjects(rowsCtx)
	}()

	return s.writeRowsToTable(rowsCtx.rows, nil)
}

func (s *shard) writeWalForArrowFlight(binary []byte) error {
	newRec := &record.Record{}
	name, err := coordinator.UnmarshalWithMeasurements(binary, newRec)
	if err != nil {
		return err
	}

	return s.writeCols(newRec, nil, name)
}

func (s *shard) setCancelWalFunc(cancel context.CancelFunc) {
	s.cancelLock.Lock()
	s.cancelFn = cancel
	s.cancelLock.Unlock()
}

func (s *shard) replayWal() error {
	// make sure the wal files exist in the disk
	s.wal.restoreLogs()

	ctx, cancel := context.WithCancel(context.Background())
	s.setCancelWalFunc(cancel)
	s.wal.ref()
	if !s.isAsyncReplayWal {
		err := s.syncReplayWal(ctx)
		s.setCancelWalFunc(nil)
		s.loadWalDone = true
		s.wal.unref()
		return err
	}
	go func() {
		defer replayWalLimit.Release()
		replayWalLimit <- struct{}{}
		err := s.syncReplayWal(ctx)
		if err != nil {
			s.log.Error("async replay wal failed", zap.Uint64("id", s.ident.ShardID), zap.Error(err))
		}
		s.setCancelWalFunc(nil)
		s.loadWalDone = true
		s.wal.unref()
	}()
	return nil
}

func (s *shard) syncReplayWal(ctx context.Context) error {
	wStart := time.Now()

	walFileNames, err := s.wal.Replay(ctx,
		func(binary []byte, rowsCtx *walRowsObjects, writeWalType WalRecordType) error {
			err := s.writeWalBuffer(binary, rowsCtx, writeWalType)
			// SeriesLimited error is ignored in the wal playback process
			if errno.Equal(err, errno.SeriesLimited) {
				err = nil
			}
			return err
		},
	)
	if err != nil {
		return err
	}
	s.log.Info("replay wal files ok", zap.Uint64("id", s.ident.ShardID), zap.Uint64("opId", s.opId), zap.Duration("time used", time.Since(wStart)))

	s.ForceFlush()
	s.log.Info("force flush shard ok", zap.Uint64("id", s.ident.ShardID), zap.Uint64("opId", s.opId), zap.Int("wal filename number", len(walFileNames)))
	err = s.wal.Remove(walFileNames)
	if err != nil {
		return err
	}
	s.log.Info("replay wal done", zap.Uint64("id", s.ident.ShardID),
		zap.Duration("time used", time.Since(wStart)), zap.Uint64("opId", s.opId))
	return nil
}

func (s *shard) Open(client metaclient.MetaClient) error {
	start := time.Now()
	s.log.Info("open shard start...", zap.Uint64("id", s.ident.ShardID), zap.Uint64("opId", s.opId))
	var err error
	if e := s.DownSampleRecover(client); e != nil {
		s.log.Error("down sample recover failed", zap.Uint64("id", s.ident.ShardID), zap.Uint64("opId", s.opId), zap.Error(e))
		return e
	}
	statistics.ShardStepDuration(s.GetID(), s.opId, "RecoverDownSample", time.Since(start).Nanoseconds(), false)
	s.immTables.SetOpId(s.GetID(), s.opId)
	maxTime, err := s.immTables.Open()
	if err != nil {
		s.log.Error("open shard failed", zap.Uint64("id", s.ident.ShardID), zap.Uint64("opId", s.opId), zap.Error(err))
		return err
	}
	s.setMaxTime(maxTime)
	s.log.Info("open immutable done", zap.Uint64("id", s.ident.ShardID), zap.Duration("time used", time.Since(start)),
		zap.Int64("maxTime", maxTime), zap.Uint64("opId", s.opId))

	s.initSeriesLimiter(s.seriesLimit)
	return nil
}

func (s *shard) IsOpened() bool {
	return s.opened
}

func (s *shard) removeFile(logFile string) error {
	lock := fileops.FileLockOption(*s.lock)
	if err := fileops.Remove(logFile, lock); err != nil {
		log.Error("remove downSample log file error", zap.Error(err))
		return err
	}
	return nil
}

func (s *shard) DownSampleRecover(client metaclient.MetaClient) error {
	shardDir := filepath.Dir(s.filesPath)
	dirs, err := fileops.ReadDir(shardDir)
	if err != nil {
		return err
	}
	for i := range dirs {
		dn := dirs[i].Name()
		if dn != immutable.DownSampleLogDir {
			continue
		}
		logDir := filepath.Join(shardDir, immutable.DownSampleLogDir)
		downSampleLogDirs, err := fileops.ReadDir(logDir)
		if err != nil {
			return err
		}
		logInfo := &DownSampleFilesInfo{}
		for _, v := range downSampleLogDirs {
			logName := v.Name()
			logFile := filepath.Join(logDir, logName)
			logInfo.reset()
			err = readDownSampleLogFile(logFile, logInfo)
			if err != nil {
				log.Error("recover downSample log file error", zap.Error(err))
				if err = s.removeFile(logFile); err != nil {
					return err
				}
				continue
			}
			err = s.DownSampleRecoverReplaceFiles(logInfo, shardDir)
			if err != nil {
				return err
			}
			s.UpdateDownSampleOnShard(logInfo.taskID, logInfo.level)
			if e := client.UpdateShardDownSampleInfo(s.ident); e != nil {
				return e
			}
			lock := fileops.FileLockOption(*s.lock)
			if err = fileops.Remove(logFile, lock); err != nil {
				log.Error("remove downSample log file error", zap.Uint64("shardID", s.ident.ShardID), zap.String("dir", shardDir), zap.String("log", logFile), zap.Error(err))
			}
		}
	}
	return nil
}

func (s *shard) DownSampleRecoverReplaceFiles(logInfo *DownSampleFilesInfo, shardDir string) error {
	for k, v := range logInfo.Names {
		mstPath := filepath.Join(shardDir, immutable.TsspDirName, v)
		dir, err := fileops.ReadDir(mstPath)
		if err != nil {
			return err
		}
		err = renameFiles(mstPath, logInfo.NewFiles[k], dir, s.lock)
		if err != nil {
			return err
		}
	}

	for k, v := range logInfo.Names {
		mstPath := filepath.Join(shardDir, immutable.TsspDirName, v)
		dir, err := fileops.ReadDir(mstPath)
		if err != nil {
			return err
		}
		err = deleteFiles(mstPath, logInfo.OldFiles[k], dir, s.lock)
		if err != nil {
			return err
		}
	}
	return nil
}

func deleteFiles(mmDir string, files []string, dirs []os.FileInfo, lockPath *string) error {
	for _, v := range files {
		for j := range dirs {
			name := dirs[j].Name()
			tmp := v + immutable.GetTmpFileSuffix()
			if name == v || tmp == name {
				fName := filepath.Join(mmDir, v)
				if _, err := fileops.Stat(fName); os.IsNotExist(err) {
					continue
				}
				lock := fileops.FileLockOption(*lockPath)
				if err := fileops.Remove(fName, lock); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func renameFiles(mmDir string, files []string, dirs []os.FileInfo, lockPath *string) error {
	for i := range dirs {
		name := dirs[i].Name()
		for k := range files {
			nameInLog := files[k]
			if nameInLog == name {
				lock := fileops.FileLockOption(*lockPath)
				normalName := nameInLog[:len(nameInLog)-len(immutable.GetTmpFileSuffix())]
				oldName := filepath.Join(mmDir, nameInLog)
				newName := filepath.Join(mmDir, normalName)
				if err := fileops.RenameFile(oldName, newName, lock); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func readDownSampleLogFile(name string, info *DownSampleFilesInfo) error {
	fi, err := fileops.Stat(name)
	if err != nil {
		return fmt.Errorf("stat downsample log file fail")
	}

	fSize := fi.Size()
	if fSize < CRCLen {
		return fmt.Errorf("too small downsample log file(%v) size %v", name, fSize)
	}

	buf := make([]byte, int(fSize))
	lock := fileops.FileLockOption("")
	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	fd, err := fileops.OpenFile(name, os.O_RDONLY, 0640, lock, pri)
	if err != nil {
		return fmt.Errorf("read downSample log file fail")
	}
	defer util.MustClose(fd)

	n, err := fd.Read(buf)
	if err != nil || n != len(buf) {
		return fmt.Errorf("read downSample log file(%v) fail, file size:%v, read size:%v", name, fSize, n)
	}
	crcValue := buf[fSize-CRCLen:]
	currCrcValue := crc32.ChecksumIEEE(buf[0 : fSize-CRCLen])
	currBytes := make([]byte, CRCLen)
	binary.BigEndian.PutUint32(currBytes, currCrcValue)
	if !bytes.Equal(crcValue, currBytes) {
		return fmt.Errorf("invalid downsample log file(%v) crc", name)
	}

	if _, err = info.unmarshal(buf); err != nil {
		return fmt.Errorf("unmarshal downSample log %v fail", name)
	}

	return nil
}

func (s *shard) OpenAndEnable(client metaclient.MetaClient) error {
	if s.IsOpened() {
		return nil
	}

	// shard can open and enable only once.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.IsOpened() {
		return nil
	}

	err := s.Open(client)
	if err != nil {
		return err
	}
	// replay wal files
	s.log.Info("replay wal start", zap.Uint64("id", s.ident.ShardID), zap.Uint64("opId", s.opId), zap.Bool("async replay", s.isAsyncReplayWal))
	wStart := time.Now()
	err = s.replayWal()
	if err != nil {
		s.log.Error("replay wal failed", zap.Uint64("id", s.ident.ShardID), zap.Error(err))
		return err
	}
	statistics.ShardStepDuration(s.GetID(), s.opId, "ReplayWalDone", time.Since(wStart).Nanoseconds(), false)
	s.log.Info("call replayWal method ok", zap.Uint64("id", s.ident.ShardID),
		zap.Duration("time used", time.Since(wStart)), zap.Uint64("opId", s.opId))

	// The shard MUST open successfully,
	// then add this shard to compaction worker.
	compWorker.RegisterShard(s)
	s.EnableDownSample()
	s.wg.Add(1)
	go s.Snapshot()
	s.opened = true
	return nil
}

func (s *shard) GetTableStore() immutable.TablesStore {
	return s.immTables
}

func (s *shard) IsOutOfOrderFilesExist() bool {
	if s.immTables == nil {
		return false
	}
	return s.immTables.IsOutOfOrderFilesExist()
}

func (s *shard) IsExpired() bool {
	now := time.Now().UTC()
	if s.durationInfo.Duration != 0 && s.endTime.Add(s.durationInfo.Duration).Before(now) {
		return true
	}
	return false
}

func (s *shard) IsTierExpired() bool {
	now := time.Now().UTC()
	if s.durationInfo.TierDuration != 0 && s.endTime.Add(s.durationInfo.TierDuration).Before(now) {
		return true
	}
	return false
}

func (s *shard) GetTier() (tier uint64) {
	return s.durationInfo.Tier
}

func (s *shard) ChangeShardTierToWarm() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.tier == util.Warm {
		return
	}
	s.immTables.FreeAllMemReader()
	s.tier = util.Warm
}

func (s *shard) GetRPName() string {
	return s.ident.Policy
}

func (s *shard) DisableDownSample() {
	if atomic.CompareAndSwapInt32(&s.downSampleEn, 1, 0) {
		close(s.stopDownSample)
	}
	s.dswg.Wait()
}

func (s *shard) EnableDownSample() {
	if atomic.CompareAndSwapInt32(&s.downSampleEn, 0, 1) {
		s.stopDownSample = make(chan struct{})
	}
}

func (s *shard) CanDoDownSample() bool {
	if s.isClosing() || !s.downSampleEnabled() {
		return false
	}
	return true
}

func (s *shard) downSampleEnabled() bool {
	return atomic.LoadInt32(&s.downSampleEn) == 1
}

// notify async goroutines to cancel the wal replay
func (s *shard) cancelWalReplay() {
	s.cancelLock.RLock()
	if s.cancelFn == nil {
		s.cancelLock.RUnlock()
		return
	}
	s.cancelFn()
	s.cancelLock.RUnlock()
	s.wal.wait()
}

func (s *shard) StartDownSample(taskID uint64, level int, sdsp *meta.ShardDownSamplePolicyInfo, meta interface {
	UpdateShardDownSampleInfo(Ident *meta.ShardIdentifier) error
}) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, schemas, logger := s.shardDownSampleTaskInfo.sdsp, s.shardDownSampleTaskInfo.schema, s.shardDownSampleTaskInfo.log
	var err error

	s.dswg.Add(1)
	defer s.dswg.Done()

	if !s.downSampleEnabled() {
		return nil
	}
	s.DisableCompAndMerge()

	lcLog := Log.NewLogger(errno.ModuleDownSample).SetZapLogger(logger)
	taskNum := len(schemas)
	parallelism := maxDownSampleTaskNum
	filesMap := make(map[int]*immutable.TSSPFiles, taskNum)
	allDownSampleFiles := make(map[int][]immutable.TSSPFile, taskNum)
	logger.Info("DownSample Start", zap.Any("shardId", info.ShardId))
	for i := 0; i < taskNum; i += parallelism {
		var num int
		if i+parallelism <= taskNum {
			num = parallelism
		} else {
			num = taskNum - i
		}
		err = s.StartDownSampleTaskBySchema(i, filesMap, allDownSampleFiles, schemas[i:i+num], info, logger)
		if err != nil {
			break
		}
	}

	if !s.downSampleEnabled() {
		err = fmt.Errorf("downsample cancel")
	}
	if err == nil {
		mstNames := make([]string, 0)
		originFiles := make([][]immutable.TSSPFile, 0)
		newFiles := make([][]immutable.TSSPFile, 0)
		for k, v := range allDownSampleFiles {
			nameWithVer := schemas[k].Options().OptionsName()
			files := filesMap[k]
			var filesSlice []immutable.TSSPFile
			for _, f := range files.Files() {
				filesSlice = append(filesSlice, f)
				f.UnrefFileReader()
				f.Unref()
			}
			mstNames = append(mstNames, nameWithVer)
			originFiles = append(originFiles, filesSlice)
			newFiles = append(newFiles, v)
		}
		if e := s.ReplaceDownSampleFiles(mstNames, originFiles, newFiles, lcLog, taskID, level, sdsp, meta); e != nil {
			s.DeleteDownSampleFiles(allDownSampleFiles)
			return e
		}
		logger.Info("DownSample Success", zap.Any("shardId", info.ShardId))
	} else {
		for _, v := range filesMap {
			for _, f := range v.Files() {
				f.UnrefFileReader()
				f.Unref()
			}
		}
		s.DeleteDownSampleFiles(allDownSampleFiles)
	}
	return err
}

func (s *shard) DeleteDownSampleFiles(allDownSampleFiles map[int][]immutable.TSSPFile) {
	for _, mstFiles := range allDownSampleFiles {
		for _, file := range mstFiles {
			fname := file.Path()
			if e := file.Remove(); e != nil {
				log.Error("remove downSample fail error", zap.String("name", fname), zap.Error(e))
			}
		}
	}
}

func (s *shard) ReplaceDownSampleFiles(mstNames []string, originFiles [][]immutable.TSSPFile, newFiles [][]immutable.TSSPFile,
	log *Log.Logger, taskID uint64, level int, sdsp *meta.ShardDownSamplePolicyInfo,
	meta interface {
		UpdateShardDownSampleInfo(Ident *meta.ShardIdentifier) error
	}) (err error) {
	if len(mstNames) == 0 || len(originFiles) == 0 || len(newFiles) == 0 {
		s.UpdateDownSampleOnShard(sdsp.TaskID, sdsp.DownSamplePolicyLevel)
		s.updateShardIentOnMeta(meta)
		return nil
	}
	if len(mstNames) != len(originFiles) || len(mstNames) != len(newFiles) {
		return fmt.Errorf("length of mst are not equal with files")
	}
	var logFile string
	logFile, err = s.writeDownSampleInfo(mstNames, originFiles, newFiles, taskID, level)
	if err != nil {
		if len(logFile) > 0 {
			lock := fileops.FileLockOption(*s.lock)
			_ = fileops.Remove(logFile, lock)
		}
		return err
	}
	s.updateDownSampleStat(originFiles, newFiles)
	if err = s.immTables.ReplaceDownSampleFiles(mstNames, originFiles, newFiles, true, func() {
		s.UpdateDownSampleOnShard(sdsp.TaskID, sdsp.DownSamplePolicyLevel)
	}); err != nil {
		return err
	}
	s.updateShardIentOnMeta(meta)
	lock := fileops.FileLockOption(*s.lock)
	if err = fileops.Remove(logFile, lock); err != nil {
		return err
	}
	return nil
}

func (s *shard) updateDownSampleStat(originFIles, newFiles [][]immutable.TSSPFile) {
	downSampleStatItem := statistics.NewDownSampleStatItem(s.ident.ShardID, s.ident.DownSampleID)
	downSampleStatItem.Level = int64(s.ident.DownSampleLevel)
	for i := range newFiles {
		downSampleStatItem.OriginalFileCount += int64(len(originFIles[i]))
		downSampleStatItem.DownSampleFileCount += int64(len(newFiles[i]))
		downSampleStatItem.OriginalFileSize += immutable.SumFilesSize(originFIles[i])
		downSampleStatItem.DownSampleFileSize += immutable.SumFilesSize(newFiles[i])
	}
	downSampleStatItem.Push()
}

func (s *shard) updateShardIentOnMeta(meta interface {
	UpdateShardDownSampleInfo(Ident *meta.ShardIdentifier) error
}) {
	var err error
	var retryCount int32
	for retryCount <= MaxRetryUpdateOnShardNum {
		retryCount++
		err = meta.UpdateShardDownSampleInfo(s.ident)
		if err == nil {
			return
		}
		time.Sleep(time.Second)
	}
	panic("update shard Ident to meta fail")
}

func (s *shard) writeDownSampleInfo(mstNames []string, originFiles [][]immutable.TSSPFile, newFiles [][]immutable.TSSPFile, taskID uint64, level int) (string, error) {
	shardDir := filepath.Dir(s.filesPath)
	info := &DownSampleFilesInfo{
		taskID:   taskID,
		level:    level,
		Names:    mstNames,
		OldFiles: make([][]string, len(originFiles)),
		NewFiles: make([][]string, len(newFiles)),
	}
	for k := range originFiles {
		info.OldFiles[k] = make([]string, len(originFiles[k]))
		for mk, f := range originFiles[k] {
			info.OldFiles[k][mk] = filepath.Base(f.Path())
		}
	}
	for k := range newFiles {
		info.NewFiles[k] = make([]string, len(newFiles[k]))
		for mk, f := range newFiles[k] {
			info.NewFiles[k][mk] = filepath.Base(f.Path())
		}
	}
	fDir := filepath.Join(shardDir, immutable.DownSampleLogDir)
	lock := fileops.FileLockOption(*s.lock)
	if err := fileops.MkdirAll(fDir, 0750, lock); err != nil {
		return "", err
	}
	fName := filepath.Join(fDir, immutable.GenLogFileName(&downSampleLogSeq))

	buf := bufferpool.Get()
	defer bufferpool.Put(buf)

	buf = info.marshal(buf[:0])

	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	fd, err := fileops.OpenFile(fName, os.O_CREATE|os.O_WRONLY, 0640, lock, pri)
	if err != nil {
		return "", err
	}

	newWriter := bufio.NewWriterSize(fd, BufferSize)

	sBuf, err := newWriter.Write(buf)
	if err != nil || sBuf != len(buf) {
		panic(err)
	}
	if err = newWriter.Flush(); err != nil {
		return "", err
	}

	if err = fd.Sync(); err != nil {
		panic(err)
	}

	return fName, fd.Close()
}

func (s *shard) StartDownSampleTaskBySchema(start int, filesMap map[int]*immutable.TSSPFiles, allDownSampleFiles map[int][]immutable.TSSPFile, schemas []hybridqp.Catalog, info *meta.ShardDownSamplePolicyInfo, logger *zap.Logger) error {
	var mstTaskNum int32
	var err error
	ch := executor.NewDownSampleStatePort(nil)
	defer ch.Close()
	downSampleStatItem := statistics.NewDownSampleStatistics()
	s.dswg.Add(len(schemas))
	downSampleStatItem.AddActive(int64(len(schemas)))
	for i := range schemas {
		mstName := schemas[i].Options().OptionsName()
		files, isExist := s.GetTSSPFiles(mstName, true)
		if !isExist || files.Len() == 0 {
			s.dswg.Done()
			downSampleStatItem.AddActive(-1)
			continue
		}
		e := s.StartDownSampleTask(start+i, mstName, files, ch, schemas[i].(*executor.QuerySchema), info.DbName, info.RpName)
		if e != nil {
			for _, v := range files.Files() {
				v.UnrefFileReader()
				v.Unref()
			}
			err = e
			logger.Warn(e.Error(), zap.Any("shardId", info.ShardId))
			s.dswg.Done()
			downSampleStatItem.AddActive(-1)
			downSampleStatItem.AddErrors(1)
			continue
		}
		logger.Info("DownSample Measurement Start",
			zap.String("Measurement", mstName), zap.Uint64("shard", info.ShardId))
		mstTaskNum += 1
		filesMap[start+i] = files
	}
	if mstTaskNum == 0 {
		return err
	}
	for {
		state := <-ch.State

		mstTaskNum -= 1
		s.dswg.Done()
		downSampleStatItem.AddActive(-1)
		taskID := state.GetTaskID()
		allDownSampleFiles[taskID] = state.GetNewFiles()
		if state.GetErr() != nil {
			err = state.GetErr()
			downSampleStatItem.AddErrors(1)
			if mstTaskNum == 0 {
				return err
			}
			continue
		}
		logger.Info("DownSample Measurement Success",
			zap.Any("Measurement", schemas[taskID-start].Options().OptionsName()),
			zap.Uint64("shard", info.ShardId))
		if mstTaskNum == 0 {
			return err
		}
	}
}

func (s *shard) GetNewFilesSeqs(files []immutable.TSSPFile) []uint64 {
	var curSeq uint64
	var curNewSeq uint64
	var newSeqs []uint64
	for k, v := range files {
		_, seq := v.LevelAndSequence()
		if k == 0 || seq != curSeq {
			newSeq := s.immTables.NextSequence()
			curNewSeq = newSeq
			curSeq = seq
		}
		newSeqs = append(newSeqs, curNewSeq)
	}
	return newSeqs
}

func (s *shard) StartDownSampleTask(taskID int, mstName string, files *immutable.TSSPFiles, port *executor.DownSampleStatePort, querySchema *executor.QuerySchema, db string, rpName string) error {
	node := executor.NewLogicalTSSPScan(querySchema)
	newSeqs := s.GetNewFilesSeqs(files.Files())
	node.SetNewSeqs(newSeqs)
	node.SetFiles(files)
	node2 := executor.NewLogicalWriteIntoStorage(node, querySchema)
	var mmsTables *immutable.MmsTables
	var ok bool
	if mmsTables, ok = s.GetTableStore().(*immutable.MmsTables); !ok {
		return fmt.Errorf("Get MmsTables error")
	}
	node2.SetMmsTables(mmsTables)
	source := influxql.Sources{&influxql.Measurement{Database: db, RetentionPolicy: rpName, Name: mstName}}
	sidSequenceReader := NewTsspSequenceReader(nil, nil, nil, source, querySchema, files, newSeqs, s.stopDownSample)
	writeIntoStorage := NewWriteIntoStorageTransform(nil, nil, nil, source, querySchema, immutable.GetTsStoreConfig(), mmsTables, s.ident.DownSampleLevel == 0)
	fileSequenceAgg := NewFileSequenceAggregator(querySchema, s.ident.DownSampleLevel == 0, s.startTime.UnixNano(), s.endTime.UnixNano())
	sidSequenceReader.GetOutputs()[0].Connect(fileSequenceAgg.GetInputs()[0])
	fileSequenceAgg.GetOutputs()[0].Connect(writeIntoStorage.GetInputs()[0])
	port.ConnectStateReserve(writeIntoStorage.GetOutputs()[0])
	writeIntoStorage.(*WriteIntoStorageTransform).SetTaskId(taskID)
	ctx := context.Background()
	go func() { _ = sidSequenceReader.Work(ctx) }()
	go func() { _ = fileSequenceAgg.Work(ctx) }()
	go func() { _ = writeIntoStorage.Work(ctx) }()
	return nil
}

func (s *shard) SetMstInfo(mstInfo *meta.MeasurementInfo) {
	name := stringinterner.InternSafe(mstInfo.Name)
	s.storage.SetMstInfo(s, name, mstInfo)
}

func (s *shard) GetID() uint64 {
	return s.ident.ShardID
}

func (s *shard) GetIdent() *meta.ShardIdentifier {
	return s.ident
}

func (s *shard) GetDuration() *meta.DurationDescriptor {
	return s.durationInfo
}

func (s *shard) GetDataPath() string {
	return s.dataPath
}

func (s *shard) GetWalPath() string {
	return s.walPath
}

func (s *shard) LastWriteTime() uint64 {
	return atomic.LoadUint64(&s.lastWriteTime)
}

// DropMeasurement drop measurement name from shard
func (s *shard) DropMeasurement(ctx context.Context, name string) error {
	if !s.loadWalDone {
		return fmt.Errorf("async replay wal not finish")
	}
	s.DisableDownSample()
	defer s.EnableDownSample()
	s.setMstDeleting(name)
	defer s.clearMstDeleting(name)
	s.mu.RLock()
	defer s.mu.RUnlock()

	// flush measurement data in mem
	s.ForceFlush()

	// drop measurement from immutable
	return s.immTables.DropMeasurement(ctx, name)
}

func (s *shard) GetStatistics(buffer []byte) ([]byte, error) {
	s.mu.RLock()
	if s.closed.Closed() {
		s.mu.RUnlock()
		return buffer, nil
	}
	fileStat := s.immTables.GetMstFileStat()
	s.mu.RUnlock()

	return s.fileStat.Collect(buffer, s.defaultTags, fileStat)
}

func (s *shard) GetShardDownSamplePolicy(policy *meta.DownSamplePolicyInfo) *meta.ShardDownSamplePolicyInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now().UTC()
	if !downSampleInorder {
		for i := len(policy.DownSamplePolicies) - 1; i >= 0; i-- {
			if s.checkDownSample(policy.TaskID, policy.DownSamplePolicies[i], i, now) {
				return &meta.ShardDownSamplePolicyInfo{
					RpName:                s.GetRPName(),
					DownSamplePolicyLevel: i + 1,
					TaskID:                policy.TaskID,
				}
			}
		}
	} else {
		for i := 0; i < len(policy.DownSamplePolicies); i++ {
			if s.checkDownSample(policy.TaskID, policy.DownSamplePolicies[i], i, now) {
				return &meta.ShardDownSamplePolicyInfo{
					RpName:                s.GetRPName(),
					DownSamplePolicyLevel: i + 1,
					TaskID:                policy.TaskID,
				}
			}
		}
	}

	return nil
}

func (s *shard) checkDownSample(id uint64, p *meta.DownSamplePolicy, index int, now time.Time) bool {
	if !s.checkDownSampleID(id) {
		return false
	}
	if !s.endTime.Add(p.SampleInterval).Before(now) {
		return false
	}
	if index <= s.ident.DownSampleLevel-1 {
		return false
	}
	return true
}

func (s *shard) checkDownSampleID(id uint64) bool {
	if s.ident.DownSampleID == 0 {
		return true
	}
	return s.ident.DownSampleID == id
}

func (s *shard) SetShardDownSampleLevel(i int) {
	s.ident.DownSampleLevel = i
}

func (s *shard) UpdateDownSampleOnShard(id uint64, level int) {
	s.GetIdent().DownSampleLevel = level
	s.GetIdent().DownSampleID = id
}

func (s *shard) NewDownSampleTask(sdsp *meta.ShardDownSamplePolicyInfo, schema []hybridqp.Catalog, log *zap.Logger) {
	s.shardDownSampleTaskInfo = &shardDownSampleTaskInfo{
		sdsp:   sdsp,
		schema: schema,
		log:    log,
	}
}

func (s *shard) UpdateShardReadOnly(meta interface {
	UpdateShardDownSampleInfo(Ident *meta.ShardIdentifier) error
}) error {
	s.ident.ReadOnly = true
	var retryNum int
	var success bool
	for retryNum < MaxRetryUpdateOnShardNum {
		retryNum++
		if e := meta.UpdateShardDownSampleInfo(s.ident); e == nil {
			success = true
			break
		}
		time.Sleep(time.Second)
	}
	if success {
		return nil
	}
	return errno.NewError(errno.UpdateShardIdentFail)
}

func (s *shard) SetIndexBuilder(builder *tsi.IndexBuilder) {
	s.indexBuilder = builder
}

func (s *shard) setMstDeleting(mst string) {
	s.droppedMst.Store(mst, struct{}{})
}

func (s *shard) clearMstDeleting(mst string) {
	s.droppedMst.Delete(mst)
}

func (s *shard) checkMstDeleting(mst string) bool {
	_, ok := s.droppedMst.Load(mst)
	return ok
}

func (s *shard) GetIndexInfo(schema *executor.QuerySchema) (*executor.AttachedIndexInfo, error) {
	// get the source measurement.
	mst := schema.Options().GetSourcesNames()[0]

	// get the data files by the measurement
	dataFileRes, ok := s.immTables.GetCSFiles(mst)
	if !ok {
		s.log.Warn(fmt.Sprintf("ScanWithSparseIndex have not data file. mst: %s, shardID: %d", mst, s.GetID()))
		return executor.NewAttachedIndexInfo(nil, nil), nil
	}
	dataFiles := dataFileRes.Files()

	// get the pk infos by the measurement
	pkInfos := make([]*colstore.PKInfo, 0, len(dataFiles))
	files := make([]immutable.TSSPFile, 0, len(dataFiles))
	for i := range dataFiles {
		dataFileName := dataFiles[i].Path()
		pkFileName := colstore.AppendPKIndexSuffix(immutable.RemoveTsspSuffix(dataFileName))
		if pkInfo, ok := s.immTables.GetPKFile(mst, pkFileName); ok {
			pkInfos = append(pkInfos, pkInfo)
			files = append(files, dataFiles[i])
		} else {
			dataFiles[i].UnrefFileReader()
			dataFiles[i].Unref()
		}
	}
	return executor.NewAttachedIndexInfo(files, pkInfos), nil
}

func (s *shard) ScanWithSparseIndex(ctx context.Context, schema *executor.QuerySchema, callBack func(num int64) error) (*executor.FileFragments, error) {
	if len(schema.Options().GetSourcesNames()) != 1 {
		return nil, fmt.Errorf("currently, Only a single table is supported")
	}

	// get the source measurement.
	mst := schema.Options().GetSourcesNames()[0]

	// get the data files by the measurement
	dataFileRes, ok := s.immTables.GetCSFiles(mst)
	if !ok {
		s.log.Warn(fmt.Sprintf("ScanWithSparseIndex have not data file. mst: %s, shardID: %d", mst, s.GetID()))
		return nil, nil
	}
	dataFiles := dataFileRes.Files()

	// get the shard fragments by the primary index and skip index
	fileFrags, skipFileIdx, err := s.scanWithSparseIndex(dataFiles, schema, mst)
	if err != nil {
		for i := range dataFiles {
			dataFiles[i].UnrefFileReader()
			dataFiles[i].Unref()
		}
		return nil, err
	}
	for _, idx := range skipFileIdx {
		dataFiles[idx].UnrefFileReader()
		dataFiles[idx].Unref()
	}
	return fileFrags, nil
}

func (s *shard) scanWithSparseIndex(dataFiles []immutable.TSSPFile, schema *executor.QuerySchema, mst string) (*executor.FileFragments, []int, error) {
	var initCondition bool
	var skipFileIdx []int
	var err error
	var keyCondition sparseindex.KeyCondition
	preTcIdx := colstore.DefaultTCLocation
	condition := schema.Options().GetCondition()
	tr := util.TimeRange{Min: schema.Options().GetStartTime(), Max: schema.Options().GetEndTime()}
	filesFragments := executor.NewFileFragments()
	mstInfo := schema.Options().GetMeasurements()[0]

	var SKFileReader []sparseindex.SKFileReader
	for i, dataFile := range dataFiles {
		dataFileName := dataFile.Path()
		pkFileName := colstore.AppendPKIndexSuffix(immutable.RemoveTsspSuffix(dataFileName))
		pkInfo, ok := s.immTables.GetPKFile(mst, pkFileName)
		if !ok {
			// If the system is powered off abnormally, the index file may not be flushed to disks.
			// When the system is powered on, the file can be played back based on the WAL and data can be read normally.
			s.log.Warn(fmt.Sprintf("ScanWithSparseIndex have no primary index file. mst: %s, shardID: %d, file: %s", mst, s.GetID(), pkFileName))
		}
		if tcIdx := pkInfo.GetTCLocation(); !initCondition || (tcIdx > colstore.DefaultTCLocation && tcIdx != preTcIdx) {
			pkSchema := pkInfo.GetRec().Schema
			tIdx := pkSchema.FieldIndex(record.TimeField)
			timePrimaryCond := binaryfilterfunc.GetTimeCondition(tr, pkSchema, tIdx)
			timeClusterCond := binaryfilterfunc.GetTimeCondition(tr, pkSchema, int(tcIdx))
			timeCondition := binaryfilterfunc.CombineConditionWithAnd(timePrimaryCond, timeClusterCond)
			keyCondition, err = sparseindex.NewKeyCondition(timeCondition, condition, pkSchema)
			if err != nil {
				return nil, nil, err
			}
			SKFileReader, err = s.skIndexReader.CreateSKFileReaders(schema.Options(), mstInfo, true)
			if err != nil {
				return nil, nil, err
			}
			initCondition = true
		}
		if schema.Options().IsTimeSorted() {
			ok, err = dataFile.ContainsByTime(tr)
			if err != nil {
				return nil, nil, fmt.Errorf("data file contain by time error")
			}
			if !ok {
				skipFileIdx = append(skipFileIdx, i)
				continue
			}
		}
		var fragmentRanges fragment.FragmentRanges
		fragmentRanges, err = s.pkIndexReader.Scan(pkFileName, pkInfo.GetRec(), pkInfo.GetMark(), keyCondition)
		if err != nil {
			return nil, nil, err
		}
		if fragmentRanges.Empty() {
			skipFileIdx = append(skipFileIdx, i)
			continue
		}
		for j := range SKFileReader {
			if err = SKFileReader[j].ReInit(dataFile); err != nil {
				return nil, nil, err
			}
			fragmentRanges, err = s.skIndexReader.Scan(SKFileReader[j], fragmentRanges)
			if err != nil {
				return nil, nil, err
			}
			if fragmentRanges.Empty() {
				skipFileIdx = append(skipFileIdx, i)
				break
			}
		}
		if len(skipFileIdx) > 0 && skipFileIdx[len(skipFileIdx)-1] == i {
			continue
		}
		var fragmentCount uint32
		for j := range fragmentRanges {
			fragmentCount += fragmentRanges[j].End - fragmentRanges[j].Start
		}
		if fragmentCount == 0 {
			skipFileIdx = append(skipFileIdx, i)
			continue
		}
		filesFragments.AddFileFragment(dataFile.Path(), executor.NewFileFragment(dataFile, fragmentRanges, int64(fragmentCount)), int64(fragmentCount))
	}
	if filesFragments.FragmentCount == 0 {
		return nil, skipFileIdx, nil
	}
	return filesFragments, skipFileIdx, nil
}

func (s *shard) RowCount(schema *executor.QuerySchema) (int64, error) {
	if len(schema.Options().GetSourcesNames()) != 1 {
		return 0, fmt.Errorf("currently, Only a single table is supported")
	}

	// get the source measurement.
	mst := schema.Options().GetSourcesNames()[0]

	// get the row count of the measurement
	mstRowCount, ok := s.msRowCount.Load(mst)
	if !ok {
		return 0, fmt.Errorf("get the row count failed. shardId: %d, mst: %s", s.GetID(), mst)
	}

	// the type of the row count is int64
	rowCount, ok := mstRowCount.(*int64)
	if !ok {
		return 0, fmt.Errorf("the type of the row count should be int64. mst: %s", mst)
	}
	return *rowCount, nil
}

func (s *shard) GetEngineType() config.EngineType {
	return s.engineType
}

func (s *shard) SetLockPath(lock *string) {
	s.lock = lock
	s.immTables.SetLockPath(lock)
}

var (
	_ Shard = (*shard)(nil)
)

func decodeShardPath(shardPath string) (database, retentionPolicy string) {
	// shardPath format: /opt/tsdb/data/data/db0/1/autogen/6_1650844800000000000_1650931200000000000_6/

	// Discard the last part of the path, remain: /opt/tsdb/data/data/db0/1/autogen/
	path, _ := filepath.Split(filepath.Clean(shardPath))

	// remain: /opt/tsdb/data/data/db0/1/
	// rp: autogen
	path, rp := filepath.Split(filepath.Clean(path))

	// remain: /opt/tsdb/data/data/db0/
	path, _ = filepath.Split(filepath.Clean(path))

	// remain: /opt/tsdb/data/data
	// db: db0
	_, db := filepath.Split(filepath.Clean(path))

	return db, rp
}
