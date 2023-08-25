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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/interruptsignal"
	"github.com/openGemini/openGemini/lib/logger"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/resourceallocator"
	stat "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/sysinfo"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

var log *logger.Logger
var openShardsLimit limiter.Fixed
var replayWalLimit limiter.Fixed

func init() {
	log = logger.NewLogger(errno.ModuleStorageEngine)
	netstorage.RegisterNewEngineFun(config.EngineType1, NewEngine)
}

type Engine struct {
	mu       sync.RWMutex // read/write lock for Engine
	closed   *interruptsignal.InterruptSignal
	dataPath string
	walPath  string
	ReadOnly bool

	engOpt       netstorage.EngineOptions
	DBPartitions map[string]map[uint32]*DBPTInfo

	log *logger.Logger

	loadCtx     *meta.LoadCtx
	dropMu      sync.RWMutex
	droppingDB  map[string]string
	droppingRP  map[string]string
	droppingMst map[string]string

	DownSamplePolicies map[string]*meta2.StoreDownSamplePolicy

	statCount int64

	mgtLock       sync.RWMutex // lock for migration
	migratingDbPT map[string]map[uint32]struct{}
	metaClient    meta.MetaClient
}

const maxInt = int(^uint(0) >> 1)

func sysTotalMemory() int {
	sysTotalMem, err := sysinfo.TotalMemory()
	if err != nil {
		panic(err)
	}

	totalMem := maxInt
	if sysTotalMem < uint64(maxInt) {
		totalMem = int(sysTotalMem)
	}

	mem := cgroup.GetMemoryLimit()
	if mem <= 0 || int64(int(mem)) != mem || int(mem) > totalMem {
		mem = cgroup.GetHierarchicalMemoryLimit()
		if mem <= 0 || int64(int(mem)) != mem || int(mem) > totalMem {
			return totalMem
		}
	}
	return int(mem)
}

func NewEngine(dataPath, walPath string, options netstorage.EngineOptions, ctx *meta.LoadCtx) (netstorage.Engine, error) {
	eng := &Engine{
		closed:        interruptsignal.NewInterruptSignal(),
		dataPath:      dataPath,
		walPath:       walPath,
		engOpt:        options,
		DBPartitions:  make(map[string]map[uint32]*DBPTInfo, 64),
		loadCtx:       ctx,
		log:           logger.NewLogger(errno.ModuleStorageEngine),
		droppingDB:    make(map[string]string),
		droppingRP:    make(map[string]string),
		droppingMst:   make(map[string]string),
		migratingDbPT: make(map[string]map[uint32]struct{}),
	}

	eng.DownSamplePolicies = make(map[string]*meta2.StoreDownSamplePolicy)
	openShardsLimit = limiter.NewFixed(options.OpenShardLimit)
	replayWalLimit = limiter.NewFixed(options.OpenShardLimit)

	SetFullCompColdDuration(options.FullCompactColdDuration)
	fileops.EnableMmapRead(options.EnableMmapRead)
	fileops.EnableReadCache(options.ReadCacheLimit)
	immutable.SetMaxCompactor(options.MaxConcurrentCompactions)
	immutable.SetMaxFullCompactor(options.MaxFullCompactions)
	immutable.SetImmTableMaxMemoryPercentage(sysTotalMemory(), options.ImmTableMaxMemoryPercentage)
	immutable.SetCacheDataBlock(options.CacheDataBlock)
	immutable.SetCacheMetaData(options.CacheMetaBlock)
	immutable.SetCompactLimit(options.CompactThroughput, options.CompactThroughputBurst)
	immutable.SetSnapshotLimit(options.SnapshotThroughput, options.SnapshotThroughputBurst)
	fileops.SetBackgroundReadLimiter(options.BackgroundReadThroughput)
	immutable.SetMergeFlag4TsStore(int32(options.CompactionMethod))
	immutable.Init()

	return eng, nil
}

func (e *Engine) GetLockFile() string {
	return ""
}

func (e *Engine) Open(durationInfos map[uint64]*meta2.ShardDurationInfo, dbBriefInfos map[string]*meta2.DatabaseBriefInfo, m meta.MetaClient) error {
	e.log.Info("start open engine...")
	start := time.Now()
	defer func(tm time.Time) {
		d := time.Since(tm)
		atomic.AddInt64(&stat.EngineStat.OpenDurations, d.Nanoseconds())
		stat.UpdateEngineStatS()
		log.Info("open engine done", zap.Duration("time used", d))
	}(start)

	lockFile := fileops.FileLockOption(e.GetLockFile())

	if err := fileops.MkdirAll(e.dataPath, 0750, lockFile); err != nil {
		atomic.AddInt64(&stat.EngineStat.OpenErrors, 1)
		return err
	}

	e.setMetaClient(m)
	err := e.loadShards(durationInfos, dbBriefInfos, immutable.LOAD, m)
	if err != nil {
		atomic.AddInt64(&stat.EngineStat.OpenErrors, 1)
		return err
	}

	return nil
}

func (e *Engine) setMetaClient(m meta.MetaClient) {
	e.mu.Lock()
	e.metaClient = m
	e.mu.Unlock()
}

func (e *Engine) loadShards(durationInfos map[uint64]*meta2.ShardDurationInfo, dbBriefInfos map[string]*meta2.DatabaseBriefInfo, loadStat int, client meta.MetaClient) error {
	e.log.Info("start loadShards...", zap.Int("loadStat", loadStat))
	dataPath := path.Join(e.dataPath, config.DataDirectory)
	_, err := fileops.Stat(dataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	errChan := make(chan error)
	n := 0
	dbRpLock := make(map[string]string)
	for _, sdi := range durationInfos {
		dbRpPath := path.Join(sdi.Ident.OwnerDb, strconv.Itoa(int(sdi.Ident.OwnerPt)), sdi.Ident.Policy)
		if _, ok := dbRpLock[dbRpPath]; ok {
			continue
		}
		dbRpLock[dbRpPath] = ""
		n++
		go func(db string, pt uint32, rp string, engineType config.EngineType) {
			enableTagArray := false
			if len(dbBriefInfos) != 0 {
				if _, ok := dbBriefInfos[db]; !ok {
					e.log.Error("fail to get dbBriefInfos", zap.String("db", db), zap.Uint32("pt", pt),
						zap.String("rp", rp))
					errChan <- errno.NewError(errno.DatabaseNotFound)
					return
				}
				enableTagArray = dbBriefInfos[db].EnableTagArray
			}
			e.createDBPTIfNotExist(db, pt, enableTagArray)
			e.mu.RLock()
			dbPTInfo := e.DBPartitions[db][pt]
			e.mu.RUnlock()
			err := dbPTInfo.loadShards(0, rp, durationInfos, loadStat, client, engineType)
			if err != nil {
				e.log.Error("fail to load db rp", zap.String("db", db), zap.Uint32("pt", pt),
					zap.String("rp", rp), zap.Error(err))
			}
			errChan <- err
		}(sdi.Ident.OwnerDb, sdi.Ident.OwnerPt, sdi.Ident.Policy, config.EngineType(sdi.Ident.EngineType))
	}

	for i := 0; i < n; i++ {
		err := <-errChan
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *Engine) Close() error {
	e.mu.Lock()

	e.closed.Close()

	start := time.Now()
	log.Info("start close engine...")
	defer func(tm time.Time) {
		d := time.Since(tm)
		atomic.AddInt64(&stat.EngineStat.CloseDurations, d.Nanoseconds())
		stat.UpdateEngineStatS()
		log.Info("close engine done", zap.Duration("time used", d))
	}(start)

	wg := sync.WaitGroup{}
	for _, dbpts := range e.DBPartitions {
		wg.Add(len(dbpts))
	}

	for _, dbpts := range e.DBPartitions {
		for _, pt := range dbpts {
			go func(pt *DBPTInfo) {
				defer wg.Done()

				if err := pt.closeDBPt(); err != nil {
					atomic.AddInt64(&stat.EngineStat.CloseErrors, 1)
					log.Error("failed to close db pt", zap.String("path", pt.path))
				}
			}(pt)
		}
	}

	wg.Wait()
	e.mu.Unlock()

	for db := range e.DBPartitions {
		e.dropDBPt(db)
	}
	return nil
}

type ShardStatus struct {
	ShardId  uint64
	Opened   bool
	ReadOnly bool
}

// MarshalText keeps marshaled dict items order
func (s ShardStatus) MarshalText() (data []byte, err error) {
	ctx := fmt.Sprintf("{ShardId: %d, Opened: %t, ReadOnly: %t}", s.ShardId, s.Opened, s.ReadOnly)
	return []byte(ctx), nil
}

func (e *Engine) getShardStatus(param map[string]string) (map[string]string, error) {
	var dbName, rpName string
	if db, ok := param["db"]; ok {
		dbName = db
	}
	if rp, ok := param["rp"]; ok {
		rpName = rp
	}

	var ptId uint32 = math.MaxUint32
	if id, ok := param["pt"]; ok {
		if n, err := strconv.Atoi(id); err == nil {
			ptId = uint32(n)
		}
	}
	var shardId uint64 = math.MaxUint64
	if id, ok := param["shard"]; ok {
		if n, err := strconv.Atoi(id); err == nil {
			shardId = uint64(n)
		}
	}
	e.log.Info("query shard status", zap.String("db", dbName), zap.String("rp", rpName), zap.Uint32("pt", ptId), zap.Uint64("shard", shardId))

	resp := make(map[string][]ShardStatus)

	e.mu.RLock()
	defer e.mu.RUnlock()
	for db, partitions := range e.DBPartitions {
		if dbName != "" && dbName != db {
			continue
		}
		for pt, dbptInfo := range partitions {
			if ptId != math.MaxUint32 && ptId != pt {
				continue
			}
			dbptInfo.mu.RLock()
			for sid, shd := range dbptInfo.shards {
				if shardId != math.MaxUint64 && shardId != sid {
					continue
				}
				if rpName != "" && rpName != shd.GetRPName() {
					continue
				}

				key := fmt.Sprintf("db: %s, rp: %s, pt: %d", db, shd.GetRPName(), shd.GetID())
				value := ShardStatus{
					ShardId:  sid,
					Opened:   shd.IsOpened(),
					ReadOnly: shd.GetIdent().ReadOnly,
				}
				resp[key] = append(resp[key], value)
			}
			dbptInfo.mu.RUnlock()
		}
	}

	var result = make(map[string]string)
	for k, v := range resp {
		val, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		result[k] = string(val)
	}
	return result, nil
}

func (e *Engine) ForceFlush() {
	e.mu.RLock()
	defer e.mu.RUnlock()
	start := time.Now()
	log.Info("start force flush shard...")

	flushDBPT := func(db string, ptID uint32) error {
		err := e.checkAndAddRefPTNoLock(db, ptID)
		if err != nil {
			return err
		}
		dbPTInfo := e.DBPartitions[db][ptID]
		defer dbPTInfo.unref()
		dbPTInfo.mu.RLock()
		defer dbPTInfo.mu.RUnlock()
		for _, sh := range dbPTInfo.shards {
			sh.ForceFlush()
		}
		return nil
	}

	for db, partitions := range e.DBPartitions {
		for id := range partitions {
			err := flushDBPT(db, id)
			if err != nil {
				continue
			}
		}
	}

	d := time.Since(start)
	log.Info("shard flush done", zap.Duration("time used(s)", d))
}

func (e *Engine) SetReadOnly(readonly bool) {
	e.ReadOnly = readonly
}

func (e *Engine) UpdateShardDurationInfo(info *meta2.ShardDurationInfo) error {
	e.mu.RLock()
	if err := e.checkAndAddRefPTNoLock(info.Ident.OwnerDb, info.Ident.OwnerPt); err != nil {
		e.mu.RUnlock()
		return err
	}
	dbPT := e.DBPartitions[info.Ident.OwnerDb][info.Ident.OwnerPt]
	e.mu.RUnlock()
	defer e.unrefDBPT(info.Ident.OwnerDb, info.Ident.OwnerPt)

	dbPT.mu.RLock()
	defer dbPT.mu.RUnlock()
	shard := dbPT.shards[info.Ident.ShardID]
	if shard == nil || shard.GetIndexBuilder() == nil {
		return nil
	}
	log.Info("duration info", zap.Uint64("shardId", info.Ident.ShardID),
		zap.Uint64("shard group id", info.Ident.ShardGroupID),
		zap.Duration("duration", info.DurationInfo.Duration))
	shard.GetIdent().ShardGroupID = info.Ident.ShardGroupID
	shard.GetDuration().Duration = info.DurationInfo.Duration
	shard.GetDuration().Tier = info.DurationInfo.Tier
	shard.GetDuration().TierDuration = info.DurationInfo.TierDuration
	shard.GetIndexBuilder().SetDuration(info.DurationInfo.Duration)
	return nil
}

func (e *Engine) ExpiredShards() []*meta2.ShardIdentifier {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var res []*meta2.ShardIdentifier
	for db := range e.DBPartitions {
		for _, pti := range e.DBPartitions[db] {
			pti.mu.RLock()
			for sid := range pti.shards {
				if pti.shards[sid].IsExpired() {
					res = append(res, pti.shards[sid].GetIdent())
				}
			}
			pti.mu.RUnlock()
		}
	}
	return res
}

func (e *Engine) ExpiredIndexes() []*meta2.IndexIdentifier {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var res []*meta2.IndexIdentifier
	for db := range e.DBPartitions {
		for _, pti := range e.DBPartitions[db] {
			pti.mu.RLock()
			for idxId := range e.DBPartitions[db][pti.id].indexBuilder {
				if e.DBPartitions[db][pti.id].indexBuilder[idxId].Expired() {
					res = append(res, e.DBPartitions[db][pti.id].indexBuilder[idxId].Ident())
				}
			}
			pti.mu.RUnlock()
		}
	}
	return res
}

// todo:need confirm
func (e *Engine) DeleteShard(db string, ptId uint32, shardID uint64) error {
	e.log.Info("start delete shard...", zap.String("db", db), zap.Uint64("shardID", shardID))
	start := time.Now()
	atomic.AddInt64(&stat.EngineStat.DelShardCount, 1)
	defer func(tm time.Time) {
		d := time.Since(tm)
		atomic.AddInt64(&stat.EngineStat.DelShardDuration, d.Nanoseconds())
		stat.UpdateEngineStatS()
		e.log.Info("delete shard done", zap.String("db", db), zap.Uint64("shardID", shardID),
			zap.Duration("time used", d))
	}(start)

	e.mu.RLock()
	if err := e.checkAndAddRefPTNoLock(db, ptId); err != nil {
		e.mu.RUnlock()
		atomic.AddInt64(&stat.EngineStat.DelShardErr, 1)
		return err
	}
	dbPtInfo := e.DBPartitions[db][ptId]
	e.mu.RUnlock()

	defer e.unrefDBPT(db, ptId)

	dbPtInfo.mu.Lock()
	if !dbPtInfo.bgrEnabled {
		dbPtInfo.mu.Unlock()
		return errno.NewError(errno.PtIsAlreadyMigrating)
	}
	sh, ok := dbPtInfo.shards[shardID]
	if !ok {
		dbPtInfo.mu.Unlock()
		return errno.NewError(errno.ShardNotFound, shardID)
	}

	if _, ok := dbPtInfo.pendingShardDeletes[shardID]; ok {
		dbPtInfo.mu.Unlock()
		return fmt.Errorf("shard %d already in deleting", shardID)
	}
	// remove from pt map
	delete(dbPtInfo.shards, shardID)
	dbPtInfo.pendingShardDeletes[shardID] = struct{}{}
	dbPtInfo.mu.Unlock()

	defer func(pt *DBPTInfo) {
		pt.mu.Lock()
		delete(pt.pendingShardDeletes, shardID)
		pt.mu.Unlock()
	}(dbPtInfo)

	// start close shard and release resource
	if err := sh.Close(); err != nil {
		atomic.AddInt64(&stat.EngineStat.DelShardErr, 1)
		return err
	}

	lock := fileops.FileLockOption(*dbPtInfo.lockPath)
	// remove shard's wal&data on-disk, index data will not delete right now
	if err := fileops.RemoveAll(sh.GetDataPath(), lock); err != nil {
		atomic.AddInt64(&stat.EngineStat.DelShardErr, 1)
		return err
	}
	if err := fileops.RemoveAll(sh.GetWalPath(), lock); err != nil {
		atomic.AddInt64(&stat.EngineStat.DelShardErr, 1)
		return err
	}

	return nil
}

func (e *Engine) DeleteIndex(db string, ptId uint32, indexID uint64) error {
	e.log.Info("start delete index...", zap.String("db", db), zap.Uint32("pt", ptId), zap.Uint64("indexID", indexID))
	start := time.Now()
	atomic.AddInt64(&stat.EngineStat.DelIndexCount, 1)
	defer func(tm time.Time) {
		d := time.Since(tm)
		atomic.AddInt64(&stat.EngineStat.DelIndexDuration, d.Nanoseconds())
		stat.UpdateEngineStatS()
		e.log.Info("delete index done", zap.String("db", db), zap.Uint64("indexID", indexID),
			zap.Duration("time used", d))
	}(start)

	e.mu.RLock()
	if err := e.checkAndAddRefPTNoLock(db, ptId); err != nil {
		e.mu.RUnlock()
		atomic.AddInt64(&stat.EngineStat.DelIndexErr, 1)
		return err
	}
	dbPtInfo := e.DBPartitions[db][ptId]
	e.mu.RUnlock()

	defer e.unrefDBPT(db, ptId)

	dbPtInfo.mu.Lock()
	iBuild, ok := dbPtInfo.indexBuilder[indexID]
	if !ok {
		dbPtInfo.mu.Unlock()
		return errno.NewError(errno.IndexNotFound, db, ptId, indexID)
	}

	if _, ok := dbPtInfo.pendingIndexDeletes[indexID]; ok {
		dbPtInfo.mu.Unlock()
		return fmt.Errorf("index %d already in deleting", indexID)
	}

	delete(dbPtInfo.indexBuilder, indexID)
	dbPtInfo.pendingIndexDeletes[indexID] = struct{}{}
	dbPtInfo.mu.Unlock()

	defer func(pt *DBPTInfo) {
		pt.mu.Lock()
		delete(pt.pendingIndexDeletes, indexID)
		pt.mu.Unlock()
	}(dbPtInfo)

	if err := iBuild.Close(); err != nil {
		atomic.AddInt64(&stat.EngineStat.DelIndexErr, 1)
		return err
	}

	lock := fileops.FileLockOption(*dbPtInfo.lockPath)
	if err := fileops.RemoveAll(iBuild.Path(), lock); err != nil {
		atomic.AddInt64(&stat.EngineStat.DelIndexErr, 1)
		return err
	}

	iBuild = nil

	return nil
}

func (e *Engine) FetchShardsNeedChangeStore() (shardsToWarm, shardsToCold []*meta2.ShardIdentifier) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for db := range e.DBPartitions {
		for pt := range e.DBPartitions[db] {
			for _, shard := range e.DBPartitions[db][pt].shards {
				tier := shard.GetTier()
				expired := shard.IsTierExpired()
				if !expired {
					continue
				}
				if tier == util.Hot {
					shardsToWarm = append(shardsToWarm, shard.GetIdent())
				} else {
					shardsToCold = append(shardsToCold, shard.GetIdent())
				}
			}
		}
	}
	return shardsToWarm, shardsToCold
}

func (e *Engine) ChangeShardTierToWarm(db string, ptId uint32, shardID uint64) error {
	log.Info("change hot shard to warm", zap.String("db", db), zap.Uint64("shardID", shardID))
	e.mu.RLock()
	if err := e.checkAndAddRefPTNoLock(db, ptId); err != nil {
		e.mu.RUnlock()
		return err
	}
	dbPtInfo := e.DBPartitions[db][ptId]
	e.mu.RUnlock()

	defer e.unrefDBPT(db, ptId)

	dbPtInfo.mu.Lock()
	sh, ok := dbPtInfo.shards[shardID]
	if !ok {
		dbPtInfo.mu.Unlock()
		return errno.NewError(errno.ShardNotFound, shardID)
	}

	if _, ok := dbPtInfo.pendingShardTiering[shardID]; ok {
		dbPtInfo.mu.Unlock()
		return fmt.Errorf("shard %d already in changing tier", shardID)
	}
	// remove from pt map
	dbPtInfo.pendingShardTiering[shardID] = struct{}{}
	dbPtInfo.mu.Unlock()

	defer func(pt *DBPTInfo) {
		pt.mu.Lock()
		delete(pt.pendingShardTiering, shardID)
		pt.mu.Unlock()
	}(dbPtInfo)

	// start change shard tier
	sh.ChangeShardTierToWarm()

	return nil
}

func (e *Engine) getShard(db string, ptId uint32, shardID uint64) (Shard, error) {
	if err := e.checkReadonly(); err != nil {
		return nil, err
	}
	e.mu.RLock()
	if err := e.checkAndAddRefPTNoLock(db, ptId); err != nil {
		e.mu.RUnlock()
		return nil, err
	}
	defer e.unrefDBPT(db, ptId)

	dbPTInfo := e.DBPartitions[db][ptId]
	e.mu.RUnlock()

	sh := dbPTInfo.Shard(shardID)
	if sh == nil {
		return nil, errno.NewError(errno.ShardNotFound, shardID)
	}
	return sh, nil
}

func (e *Engine) WriteRows(db, rp string, ptId uint32, shardID uint64, rows []influx.Row, binaryRows []byte) error {
	sh, err := e.getShard(db, ptId, shardID)
	if err != nil {
		return err
	}
	return sh.WriteRows(rows, binaryRows)
}

func (e *Engine) WriteRec(db, mst string, ptId uint32, shardID uint64, rec *record.Record, binaryRec []byte) error {
	sh, err := e.getShard(db, ptId, shardID)
	if err != nil {
		return err
	}
	return sh.WriteCols(mst, rec, binaryRec)
}

func (e *Engine) checkReadonly() error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.ReadOnly {
		return errno.NewError(errno.ErrWriteReadonly)
	}
	return nil
}

func (e *Engine) CreateShard(db, rp string, ptId uint32, shardID uint64, timeRangeInfo *meta2.ShardTimeRangeInfo, mstInfo *meta2.MeasurementInfo) error {
	e.mu.RLock()
	if err := e.checkAndAddRefPTNoLock(db, ptId); err != nil {
		e.mu.RUnlock()
		return err
	}
	dbPTInfo := e.DBPartitions[db][ptId]
	e.mu.RUnlock()
	defer e.unrefDBPT(db, ptId)

	dbPTInfo.mu.Lock()
	dbPTInfo.SetOption(e.engOpt)
	defer dbPTInfo.mu.Unlock()
	_, ok := dbPTInfo.shards[shardID]
	if !ok {
		sh, err := dbPTInfo.NewShard(rp, shardID, timeRangeInfo, e.metaClient, mstInfo.EngineType)
		if err != nil {
			return err
		}
		sh.SetMstInfo(mstInfo.Name, mstInfo)
		dbPTInfo.shards[shardID] = sh
		newestShardID, ok := dbPTInfo.newestRpShard[rp]
		if !ok || newestShardID < shardID {
			dbPTInfo.newestRpShard[rp] = shardID
		}
	}
	return nil
}

func (e *Engine) GetShardSplitPoints(db string, ptId uint32, shardID uint64, idxes []int64) ([]string, error) {
	e.mu.RLock()
	if !e.isDBPtExist(db, ptId) {
		e.mu.RUnlock()
		return nil, errno.NewError(errno.PtNotFound)
	}
	dbPtInfo := e.DBPartitions[db][ptId]
	e.mu.RUnlock()

	shard := dbPtInfo.Shard(shardID)
	if shard == nil {
		return nil, errno.NewError(errno.ShardNotFound, shardID)
	}

	return shard.GetSplitPoints(idxes)
}

func (e *Engine) isDBPtExist(db string, ptId uint32) bool {
	if dbPT, dbExist := e.DBPartitions[db]; dbExist {
		if _, dbPTExist := dbPT[ptId]; dbPTExist {
			return true
		}
	}
	return false
}

func (e *Engine) addDBPTInfo(dbPTInfo *DBPTInfo) {
	dbPT, dbExist := e.DBPartitions[dbPTInfo.database]
	if dbExist {
		if _, dbPTExist := dbPT[dbPTInfo.id]; !dbPTExist {
			dbPT[dbPTInfo.id] = dbPTInfo
		}
	} else {
		dbPT = make(map[uint32]*DBPTInfo)
		dbPT[dbPTInfo.id] = dbPTInfo
		e.DBPartitions[dbPTInfo.database] = dbPT
	}
}

func (e *Engine) dropDBPTInfo(database string, ptID uint32) {
	if dbPT, dbExist := e.DBPartitions[database]; dbExist {
		delete(dbPT, ptID)
		if len(dbPT) == 0 {
			delete(e.DBPartitions, database)
		}
	}
}

func (e *Engine) CreateDBPT(db string, pt uint32, enableTagArray bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	ptPath := path.Join(e.dataPath, config.DataDirectory, db, strconv.Itoa(int(pt)))
	walPath := path.Join(e.walPath, config.WalDirectory, db, strconv.Itoa(int(pt)))
	lockPath := path.Join(ptPath, "LOCK")
	dbPTInfo := NewDBPTInfo(db, pt, ptPath, walPath, e.loadCtx)
	dbPTInfo.lockPath = &lockPath
	e.addDBPTInfo(dbPTInfo)
	dbPTInfo.SetOption(e.engOpt)
	dbPTInfo.enableReportShardLoad()
	dbPTInfo.enableTagArray = enableTagArray
}

func (e *Engine) createDBPTIfNotExist(db string, pt uint32, enableTagArray bool) {
	e.mu.RLock()
	if e.isDBPtExist(db, pt) {
		e.mu.RUnlock()
		return
	}
	e.mu.RUnlock()
	e.CreateDBPT(db, pt, enableTagArray)
}

func (e *Engine) startDrop(name string, droppingMap map[string]string) error {
	for st := time.Now(); time.Since(st) < 5*time.Minute; {
		e.dropMu.Lock()
		if _, ok := droppingMap[name]; ok {
			e.dropMu.Unlock()
			log.Warn("concurrency delete", zap.String("del", name))
			time.Sleep(5 * time.Second)
			continue
		}
		droppingMap[name] = ""
		e.dropMu.Unlock()
		return nil
	}
	return fmt.Errorf("concurrency delete database timeout")
}

func (e *Engine) endDrop(name string, droppingMap map[string]string) {
	e.dropMu.Lock()
	delete(droppingMap, name)
	e.dropMu.Unlock()
}

func (e *Engine) getPartition(db string, ptID uint32, isRef bool) (*DBPTInfo, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	dbPT, dbExist := e.DBPartitions[db]
	if !dbExist {
		return nil, errno.NewError(errno.DatabaseNotFound, db)
	}
	pt, ptExist := dbPT[ptID]
	if !ptExist {
		return nil, errno.NewError(errno.PtNotFound)
	}
	if !isRef {
		return pt, nil
	}
	if suc := pt.ref(); suc {
		return pt, nil
	}
	return nil, errno.NewError(errno.DBPTClosed)
}

func deleteDataAndWalPath(dataPath, walPath string, lockPath *string) error {
	logger.GetLogger().Info("deleteDataAndWalPath",
		zap.String("data", dataPath), zap.String("wal", walPath))

	if err := deleteDir(dataPath, lockPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	if err := deleteDir(walPath, lockPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func deleteDir(path string, lockPath *string) error {
	lock := fileops.FileLockOption(*lockPath)
	if err := fileops.RemoveAll(path, lock); err != nil {
		return err
	}

	return nil
}

func (e *Engine) deleteShardsAndIndexes(dbPTInfo *DBPTInfo) error {
	dbPTInfo.mu.Lock()
	defer dbPTInfo.mu.Unlock()

	for id, shard := range dbPTInfo.shards {
		if err := shard.Close(); err != nil {
			return err
		}
		delete(dbPTInfo.shards, id)
	}
	for id, iBuild := range dbPTInfo.indexBuilder {
		if err := iBuild.Close(); err != nil {
			return err
		}
		delete(dbPTInfo.indexBuilder, id)
	}
	return nil
}

func (e *Engine) dropDBPt(db string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for id := range e.DBPartitions[db] {
		e.dropDBPTInfo(db, id)
	}
}

func (e *Engine) deleteIndexes(db string, pt uint32, rp string, fn func(dbPTInfo *DBPTInfo, shardID uint64, sh Shard) error) error {
	resC := make(chan error)
	indexes := make(map[uint64]struct{})

	dbPTInfo := e.getDBPTInfo(db, pt)
	var n int
	for shardId := range dbPTInfo.shards {
		if dbPTInfo.shards[shardId].GetRPName() != rp {
			continue
		}
		indexID := dbPTInfo.shards[shardId].GetIndexBuilder().GetIndexID()
		if _, ok := dbPTInfo.indexBuilder[indexID]; ok {
			indexes[indexID] = struct{}{}
		}
		n++

		go func(info *DBPTInfo, id uint64, sh Shard) {
			err := fn(info, id, sh)
			if err != nil {
				resC <- fmt.Errorf("shard %d: %s", id, err)
				return
			}
			resC <- err
		}(dbPTInfo, shardId, dbPTInfo.shards[shardId])
	}

	var err error
	for i := 0; i < n; i++ {
		res := <-resC
		if res != nil {
			err = res
		}
	}
	close(resC)

	if err != nil {
		return err
	}

	for index := range indexes {
		if e := e.deleteOneIndex(dbPTInfo, index); e != nil {
			err = e
		}
	}
	return err
}

func (e *Engine) deleteOneIndex(dbPTInfo *DBPTInfo, indexId uint64) error {
	dbPTInfo.mu.Lock()
	iBuilder, ok := dbPTInfo.indexBuilder[indexId]
	if !ok {
		dbPTInfo.mu.Unlock()
		return nil
	}
	dbPTInfo.pendingIndexDeletes[indexId] = struct{}{}
	dbPTInfo.mu.Unlock()

	err := iBuilder.Close()
	dbPTInfo.mu.Lock()
	defer dbPTInfo.mu.Unlock()
	if err == nil {
		delete(dbPTInfo.indexBuilder, indexId)
	}
	delete(dbPTInfo.pendingIndexDeletes, indexId)
	return err
}

func (e *Engine) SeriesExactCardinality(db string, ptIDs []uint32, measurements [][]byte, condition influxql.Expr) (map[string]uint64, error) {
	keysMap, err := e.searchSeries(db, ptIDs, measurements, condition)
	if err != nil {
		return nil, err
	}

	// Count all measurement series cardinality
	result := make(map[string]uint64, len(measurements))
	for _, nameBytesWithVer := range measurements {
		name := influx.GetOriginMstName(util.Bytes2str(nameBytesWithVer))
		result[name] = uint64(len(keysMap[name]))
	}
	return result, nil
}

func (e *Engine) searchSeries(db string, ptIDs []uint32, measurements [][]byte, condition influxql.Expr) (map[string]map[string]struct{}, error) {
	e.mu.RLock()
	var err error
	if ptIDs, err = e.checkAndAddRefPTSNoLock(db, ptIDs); err != nil {
		e.mu.RUnlock()
		return nil, err
	}
	defer e.unrefDBPTs(db, ptIDs)
	pts, ok := e.DBPartitions[db]
	e.mu.RUnlock()
	if !ok {
		return nil, nil
	}

	keysMap := make(map[string]map[string]struct{}, 64)
	for _, nameWithVer := range measurements {
		name := influx.GetOriginMstName(util.Bytes2str(nameWithVer))
		keysMap[name] = make(map[string]struct{}, 64)
	}
	series := make([][]byte, 1)
	seriesLen := 0
	defer func() {
		series = series[:seriesLen]
		for i := range series {
			if len(series[i]) > 0 {
				influx.PutBytesBuffer(series[i])
			}
		}
	}()
	for _, ptID := range ptIDs {
		pt, ok := pts[ptID]
		if !ok {
			continue
		}
		pt.mu.RLock()
		for _, iBuild := range pt.indexBuilder {
			for _, nameWithVer := range measurements {
				mstName := influx.GetOriginMstName(util.Bytes2str(nameWithVer))
				stime := time.Now()
				idx := iBuild.GetPrimaryIndex().(*tsi.MergeSetIndex)
				series, err = idx.SearchSeriesKeys(series[:0], nameWithVer, condition)
				if len(series) > seriesLen {
					seriesLen = len(series)
				}
				log.Info("search series keys", zap.ByteString("nameWithVer", nameWithVer),
					zap.Duration("cost", time.Since(stime)))
				if err != nil {
					pt.mu.RUnlock()
					return nil, err
				}
				stime = time.Now()
				for _, key := range series {
					key = bytes.Replace(key, nameWithVer, []byte(mstName), 1)
					keysMap[mstName][string(key)] = struct{}{}
				}
				log.Info("remove dupicate key", zap.String("nameWithVer", string(nameWithVer)),
					zap.Duration("cost", time.Since(stime)))
			}
		}
		pt.mu.RUnlock()
	}
	return keysMap, nil
}

func (e *Engine) DbPTRef(db string, ptId uint32) error {
	var err error
	e.mu.RLock()
	err = e.checkAndAddRefPTNoLock(db, ptId)
	e.mu.RUnlock()
	return err
}

func (e *Engine) DbPTUnref(db string, ptId uint32) {
	e.mu.RLock()
	e.unrefDBPTNoLock(db, ptId)
	e.mu.RUnlock()
}

func (e *Engine) GetShard(db string, ptId uint32, shardID uint64) Shard {
	pt := e.getDBPTInfo(db, ptId)
	if pt == nil {
		return nil
	}

	return pt.Shard(shardID)
}

func (e *Engine) getDBPTInfo(db string, ptId uint32) *DBPTInfo {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.DBPartitions[db][ptId]
}

func (e *Engine) CreateLogicalPlan(ctx context.Context, db string, ptId uint32, shardID uint64,
	sources influxql.Sources, schema *executor.QuerySchema) (hybridqp.QueryNode, error) {
	sh := e.GetShard(db, ptId, shardID)
	if sh == nil {
		return nil, nil
	}

	// FIXME:context cancel func
	return sh.CreateLogicalPlan(ctx, sources, schema)
}

func (e *Engine) ScanWithSparseIndex(ctx context.Context, db string, ptId uint32, shardIDs []uint64, schema *executor.QuerySchema) (executor.ShardsFragments, error) {
	shardFrags := executor.NewShardsFragments()
	for _, shardId := range shardIDs {
		s := e.GetShard(db, ptId, shardId)
		if s == nil {
			e.log.Warn(fmt.Sprintf("ScanWithSparseIndex shard is null. db: %s, ptId: %d, shardId: %d", db, ptId, shardId))
			continue
		}
		fileFrags, err := s.ScanWithSparseIndex(ctx, schema, resourceallocator.DefaultSeriesAllocateFunc)
		if err != nil {
			return nil, err
		}
		if fileFrags == nil {
			continue
		}
		shardFrags[shardId] = fileFrags
	}
	return shardFrags, nil
}

func (e *Engine) LogicalPlanCost(db string, ptId uint32, sources influxql.Sources, opt query.ProcessorOptions) (hybridqp.LogicalPlanCost, error) {
	panic("implement me")
}

func (e *Engine) checkAndAddRefPTSNoLock(database string, ptIDs []uint32) ([]uint32, error) {
	delRefPtId := make([]uint32, 0, len(ptIDs))
	var err error
	for _, ptId := range ptIDs {
		if err1 := e.checkAndAddRefPTNoLock(database, ptId); err1 != nil {
			if !config.GetHaEnable() && errno.Equal(err1, errno.PtNotFound) {
				continue
			}
			err = err1
			break
		} else {
			delRefPtId = append(delRefPtId, ptId)
		}
	}

	if err != nil {
		e.unrefDBPTSNoLock(database, delRefPtId)
	}
	return delRefPtId, err
}

func (e *Engine) checkAndAddRefPTNoLock(database string, ptID uint32) error {
	if dbPT, dbExist := e.DBPartitions[database]; dbExist {
		if _, ok := dbPT[ptID]; ok {
			if suc := e.DBPartitions[database][ptID].ref(); suc {
				return nil
			} else {
				return errno.NewError(errno.DBPTClosed)
			}
		} else {
			return errno.NewError(errno.PtNotFound)
		}
	} else {
		return errno.NewError(errno.PtNotFound)
	}
}

func (e *Engine) unrefDBPT(database string, ptID uint32) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	e.unrefDBPTNoLock(database, ptID)
}

func (e *Engine) unrefDBPTs(database string, ptIDs []uint32) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, ptID := range ptIDs {
		e.unrefDBPTNoLock(database, ptID)
	}
}

func (e *Engine) refDBPTsNoLock(ptInfo map[uint32]*DBPTInfo, db string) ([]uint32, error) {
	ptIds := make([]uint32, len(ptInfo))
	i := 0
	for id := range ptInfo {
		ptIds[i] = id
		i++
	}
	var err error
	if ptIds, err = e.checkAndAddRefPTSNoLock(db, ptIds); err != nil {
		return nil, err
	}

	return ptIds, nil
}

func (e *Engine) unrefDBPTSNoLock(database string, ptIDs []uint32) {
	for _, ptID := range ptIDs {
		e.unrefDBPTNoLock(database, ptID)
	}
}

func (e *Engine) unrefDBPTNoLock(database string, ptID uint32) {
	dbPT, dbExist := e.DBPartitions[database]
	if !dbExist {
		return
	}
	_, ok := dbPT[ptID]
	if !ok {
		log.Error("pt not found", zap.String("database", database), zap.Uint32("pt", ptID))
		panic("pt not found")
	}
	dbPT[ptID].unref()
}

func (e *Engine) SysCtrl(req *netstorage.SysCtrlRequest) (map[string]string, error) {
	return e.processReq(req)
}

func (e *Engine) Statistics(buffer []byte) ([]byte, error) {
	e.statCount++
	if stat.FileStatisticsLimited(e.statCount) {
		return nil, nil
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed.Closed() {
		return buffer, nil
	}

	for db, partitions := range e.DBPartitions {
		for id := range partitions {
			dbPTInfo := e.DBPartitions[db][id]
			dbPTInfo.mu.RLock()
			for _, sh := range dbPTInfo.shards {
				buffer, _ = sh.GetStatistics(buffer)
			}
			dbPTInfo.mu.RUnlock()
		}
	}
	return buffer, nil
}
