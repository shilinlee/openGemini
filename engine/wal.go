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
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/pingcap/failpoint"
	"go.uber.org/zap"
)

const (
	DefaultFileSize   = 10 * 1024 * 1024
	WALFileSuffixes   = "wal"
	WalRecordHeadSize = 1 + 4
	WalCompBufSize    = 256 * 1024
	WalCompMaxBufSize = 2 * 1024 * 1024
)

type WalRecordType byte

const (
	WriteWALRecord WalRecordType = 0x01
)

var (
	walCompBufPool = bufferpool.NewByteBufferPool(WalCompBufSize)
)

type WAL struct {
	log            *logger.Logger
	mu             sync.RWMutex
	wg             sync.WaitGroup // wait for replay wal finish
	logPath        string
	lock           *string
	partitionNum   int
	writeReq       uint64
	logWriter      []LogWriter
	walEnabled     bool
	replayParallel bool
}

func NewWAL(path string, lockPath *string, walSyncInterval time.Duration, walEnabled, replayParallel bool, partitionNum int, writeWal bool) *WAL {
	wal := &WAL{
		logPath:        path,
		partitionNum:   partitionNum,
		logWriter:      make([]LogWriter, partitionNum),
		walEnabled:     walEnabled,
		replayParallel: replayParallel,
		log:            logger.NewLogger(errno.ModuleWal),
		lock:           lockPath,
	}

	lock := fileops.FileLockOption(*lockPath)
	for i := 0; i < partitionNum; i++ {
		wal.logWriter[i] = LogWriter{
			closed:       make(chan struct{}),
			logPath:      filepath.Join(path, strconv.Itoa(i)),
			SyncInterval: walSyncInterval,
			lock:         lockPath,
		}
		err := fileops.MkdirAll(wal.logWriter[i].logPath, 0750, lock)
		if err != nil {
			panic(err)
		}
		wal.setLastSeqAndFiles(&wal.logWriter[i])
		if writeWal {
			wal.logWriter[i].fileNames = wal.logWriter[i].fileNames[:0]
		}
	}

	return wal
}

func (l *WAL) setLastSeqAndFiles(logWriter *LogWriter) {
	logPath := logWriter.logPath
	// read logPath
	dirs, err := fileops.ReadDir(logPath)
	if err != nil {
		panic(err)
	}
	// sort file ordered by fileSeq. sort from new to old
	sort.Slice(dirs, func(i, j int) bool {
		iLen := len(dirs[i].Name())
		jLen := len(dirs[j].Name())
		if iLen == jLen {
			return dirs[i].Name() > dirs[j].Name()
		}
		return iLen > jLen
	})
	if len(dirs) == 0 {
		return
	}
	maxSeq, err := strconv.Atoi(dirs[0].Name()[:len(dirs[0].Name())-len(WALFileSuffixes)-1])
	if err != nil {
		l.log.Error("parse wal file failed", zap.String("path", logPath), zap.Error(err))
		return
	}
	logWriter.fileSeq = maxSeq
	// traverse files from old to new
	for n := len(dirs) - 1; n >= 0; n-- {
		walFile := filepath.Join(logPath, dirs[n].Name())
		logWriter.fileNames = append(logWriter.fileNames, walFile)
	}
}

func (l *WAL) writeBinary(binaryData []byte) error {
	// prepare for compress memory
	compBuf := walCompBufPool.Get()
	maxEncodeLen := snappy.MaxEncodedLen(len(binaryData))
	compBuf = bufferpool.Resize(compBuf, WalRecordHeadSize+maxEncodeLen)
	defer func() {
		if len(compBuf) <= WalCompMaxBufSize {
			walCompBufPool.Put(compBuf)
		}
	}()

	// compress data
	compData := snappy.Encode(compBuf[WalRecordHeadSize:], binaryData)

	// encode record header
	compBuf[0] = byte(WriteWALRecord)
	binary.BigEndian.PutUint32(compBuf[1:WalRecordHeadSize], uint32(len(compData)))
	compBuf = compBuf[:WalRecordHeadSize+len(compData)]

	// write data, switch to new file if needed
	l.mu.RLock()
	err := l.logWriter[(atomic.AddUint64(&l.writeReq, 1)-1)%uint64(l.partitionNum)].Write(compBuf)
	l.mu.RUnlock()
	if err != nil {
		panic(fmt.Errorf("writing WAL entry failed: %v", err))
	}
	return nil
}

func (l *WAL) Write(binary []byte) error {
	if !l.walEnabled {
		return nil
	}
	if len(binary) == 0 {
		return nil
	}
	return l.writeBinary(binary)
}

func (l *WAL) Switch() ([]string, error) {
	if !l.walEnabled {
		return nil, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	var mu = sync.Mutex{}
	var errs []error
	var wg sync.WaitGroup
	var totalFiles []string

	wg.Add(l.partitionNum)
	for i := 0; i < l.partitionNum; i++ {
		go func(lw *LogWriter) {
			files, err := lw.Switch()

			mu.Lock()
			if err != nil {
				errs = append(errs, err)
			}
			totalFiles = append(totalFiles, files...)
			mu.Unlock()

			wg.Done()
		}(&l.logWriter[i])
	}
	wg.Wait()

	for _, err := range errs {
		return nil, err
	}

	return totalFiles, nil
}

func (l *WAL) Remove(files []string) error {
	if !l.walEnabled {
		return nil
	}
	lock := fileops.FileLockOption(*l.lock)
	for _, fn := range files {
		err := fileops.Remove(fn, lock)
		if err != nil {
			l.log.Error("failed to remove wal file", zap.String("file", fn))
			return err
		}
	}

	return nil
}

func (l *WAL) replayPhysicRecord(fd fileops.File, offset, fileSize int64, recordCompBuff []byte,
	callBack func(binary []byte) error) (int64, []byte, error) {
	// check file is all replayed
	if offset >= fileSize {
		return offset, recordCompBuff, io.EOF
	}

	// read record header
	var recordHeader [WalRecordHeadSize]byte
	n, err := fd.ReadAt(recordHeader[:], offset)
	if err != nil {
		l.log.Warn(errno.NewError(errno.ReadWalFileFailed, fd.Name(), offset, "record header").Error())
		return offset, recordCompBuff, io.EOF
	}
	if n != WalRecordHeadSize {
		l.log.Warn(errno.NewError(errno.WalRecordHeaderCorrupted, fd.Name(), offset).Error())
		return offset, recordCompBuff, io.EOF
	}
	offset += int64(len(recordHeader))

	// prepare record memory
	compBinaryLen := binary.BigEndian.Uint32(recordHeader[1:WalRecordHeadSize])
	recordCompBuff = bufferpool.Resize(recordCompBuff, int(compBinaryLen))

	// read record body
	var recordBuff []byte
	n, err = fd.ReadAt(recordCompBuff, offset)
	if err == nil || err == io.EOF {
		offset += int64(n)
		var innerErr error
		recordBuff, innerErr = snappy.Decode(recordBuff, recordCompBuff)
		if innerErr != nil {
			l.log.Warn(errno.NewError(errno.DecompressWalRecordFailed, fd.Name(), offset, innerErr.Error()).Error())
			return offset, recordCompBuff, io.EOF
		}

		innerErr = callBack(recordBuff)
		if innerErr == nil {
			return offset, recordCompBuff, err
		}

		return offset, recordCompBuff, innerErr
	}
	l.log.Warn(errno.NewError(errno.ReadWalFileFailed, fd.Name(), offset, "record body").Error())
	return offset, recordCompBuff, io.EOF
}

func (l *WAL) replayWalFile(ctx context.Context, walFileName string, callBack func(binary []byte) error) error {
	failpoint.Inject("mock-replay-wal-error", func(val failpoint.Value) {
		msg := val.(string)
		if strings.Contains(walFileName, msg) {
			failpoint.Return(fmt.Errorf(msg))
		}
	})
	lock := fileops.FileLockOption("")
	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	fd, err := fileops.OpenFile(walFileName, os.O_RDONLY, 0640, lock, pri)
	if err != nil {
		return err
	}

	stat, err := fd.Stat()
	if err != nil {
		return err
	}

	fileSize := stat.Size()
	if fileSize == 0 {
		return nil
	}

	recordCompBuff := walCompBufPool.Get()
	defer func() {
		if len(recordCompBuff) <= WalCompMaxBufSize {
			walCompBufPool.Put(recordCompBuff)
		}
		util.MustClose(fd)
	}()

	offset := int64(0)
	for {
		select {
		case <-ctx.Done():
			l.log.Info("cancel replay wal", zap.String("filename", walFileName))
			return nil
		default:
		}
		offset, recordCompBuff, err = l.replayPhysicRecord(fd, offset, fileSize, recordCompBuff, callBack)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func (l *WAL) replayOnePartition(ctx context.Context, idx int, callBack func(binary []byte) error) error {
	for _, fileName := range l.logWriter[idx].fileNames {
		select {
		case <-ctx.Done():
			l.log.Info("cancel replay wal", zap.String("filename", fileName))
			return nil
		default:
		}
		err := l.replayWalFile(ctx, fileName, callBack)
		if err != nil {
			return err
		}
	}
	return nil
}

func consumeRecordSerial(ctx context.Context, mu *sync.Mutex, ptChs []chan []byte, errs *[]error, finish chan<- struct{}, callBack func(binary []byte) error) {
	// consume wal data according to partition order from channel
	ptFinish := make([]bool, len(ptChs))
	ptFinishNum := 0
	for {
		for i := range ptChs {
			if ptFinish[i] {
				continue
			}
			var binary []byte
			select {
			case <-ctx.Done():
				finish <- struct{}{}
				return
			case binary = <-ptChs[i]:
			}
			if len(binary) == 0 {
				ptFinishNum++
				ptFinish[i] = true
				if ptFinishNum == len(ptChs) {
					finish <- struct{}{}
					return
				}
				continue
			}
			err := callBack(binary)
			if err != nil {
				mu.Lock()
				*errs = append(*errs, err)
				mu.Unlock()
			}
		}
	}
}

func consumeRecordParallel(ctx context.Context, mu *sync.Mutex, ptChs []chan []byte, errs *[]error, finish chan<- struct{}, callBack func(binary []byte) error) {
	var wg sync.WaitGroup
	wg.Add(len(ptChs))
	for i := 0; i < len(ptChs); i++ {
		go func(idx int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case binary := <-ptChs[idx]:
					if len(binary) == 0 {
						return
					}
					err := callBack(binary)
					if err != nil {
						mu.Lock()
						*errs = append(*errs, err)
						mu.Unlock()
					}
				}
			}
		}(i)
	}
	wg.Wait()
	finish <- struct{}{}
}

func (l *WAL) productRecordParallel(ctx context.Context, mu *sync.Mutex, ptChs []chan []byte, errs *[]error) []string {
	var wg sync.WaitGroup
	var walFileNames []string
	wg.Add(len(l.logWriter))
	for i := range l.logWriter {
		go func(idx int) {
			err := l.replayOnePartition(ctx, idx, func(binary []byte) error {
				ptChs[idx] <- binary
				return nil
			})
			ptChs[idx] <- nil
			mu.Lock()
			if err != nil {
				*errs = append(*errs, err)
			}
			walFileNames = append(walFileNames, l.logWriter[idx].fileNames...)
			mu.Unlock()
			wg.Done()
		}(i)
	}
	wg.Wait()
	return walFileNames
}

func (l *WAL) Replay(ctx context.Context, callBack func(binary []byte) error) ([]string, error) {
	if !l.walEnabled {
		return nil, nil
	}
	l.wg.Add(2) // productRecord and consumeRecord

	// replay wal files
	var mu = sync.Mutex{}
	var errs []error
	var ptChs []chan []byte
	consumeFinish := make(chan struct{})
	for i := 0; i < len(l.logWriter); i++ {
		ptChs = append(ptChs, make(chan []byte, 4))
	}

	if l.replayParallel {
		// multi partition wal files replayed parallel, the update of the same time at a certain series is not guaranteed
		go func() {
			defer l.wg.Done()
			consumeRecordParallel(ctx, &mu, ptChs, &errs, consumeFinish, callBack)
		}()
	} else {
		// multi partition wal files replayed serial, the update of the same time at a certain series is guaranteed
		go func() {
			defer l.wg.Done()
			consumeRecordSerial(ctx, &mu, ptChs, &errs, consumeFinish, callBack)
		}()
	}

	// production wal data to channel
	walFileNames := l.productRecordParallel(ctx, &mu, ptChs, &errs)
	l.wg.Done()
	<-consumeFinish
	close(consumeFinish)
	for i := range ptChs {
		close(ptChs[i])
	}

	for _, err := range errs {
		return nil, err
	}
	return walFileNames, nil
}

func (l *WAL) Close() error {
	if !l.walEnabled {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	var mu = sync.Mutex{}
	var errs []error
	var wg sync.WaitGroup

	wg.Add(l.partitionNum)
	for i := 0; i < l.partitionNum; i++ {
		go func(lw *LogWriter) {
			err := lw.close()
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
			wg.Done()
		}(&l.logWriter[i])
	}
	wg.Wait()

	for _, err := range errs {
		return err
	}
	return nil
}
