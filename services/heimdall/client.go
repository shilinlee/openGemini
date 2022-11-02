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
package heimdall

import (
	"bufio"
	"io"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
)

type ByteReadReader interface {
	io.Reader
	io.ByteReader
}

// write record into connection
func WriteData(record array.Record, out io.WriteCloser) error {
	w := ipc.NewWriter(out, ipc.WithSchema(record.Schema()))
	err := w.Write(record)
	if err != nil {
		return err
	}
	err = w.Close()
	if err != nil {
		return err
	}
	return nil
}

// read record from connection
func ReadData(in ByteReadReader) ([]array.Record, error) {
	rdr, err := ipc.NewReader(in)
	if err != nil {
		if strings.HasSuffix(err.Error(), ": EOF") {
			return nil, io.EOF
		}
		return nil, err
	}
	var records []array.Record
	for rdr.Next() {
		out := rdr.Record()
		out.Retain()
		records = append(records, out)
	}
	if rdr.Err() != nil {
		return nil, rdr.Err()
	}
	return records, nil
}

type heimdallCli struct {
	dataSocketIn  io.WriteCloser // write into pyworker
	dataSocketOut ByteReadReader // read from pyworker

	alive    bool
	logger   *logger.Logger
	cnt      *int32 // reference of service client quantity
	mu       sync.Mutex
	respChan chan<- array.Record
}

func newClient(addr string, logger *logger.Logger, respChan chan<- array.Record, cnt *int32) (*heimdallCli, *errno.Error) {
	conn, err := getConn(addr)
	if err != nil {
		return nil, errno.NewError(errno.FailToConnectToPyworker)
	}
	_, err = conn.Write([]byte(BATCH))
	if err != nil {
		return nil, errno.NewError(errno.FailToConnectToPyworker)
	}
	readerBuf := bufio.NewReader(conn)
	cli := &heimdallCli{
		dataSocketIn:  conn,
		dataSocketOut: readerBuf,
		alive:         true,
		logger:        logger,
		cnt:           cnt,
		respChan:      respChan,
	}
	atomic.AddInt32(cli.cnt, 1)
	go cli.Read()
	return cli, nil
}

// Write send data to through internal connection
func (h *heimdallCli) Write(rec array.Record) *errno.Error {
	if err := WriteData(rec, h.dataSocketIn); err != nil {
		defer h.Close()
		return errno.NewThirdParty(err, errno.ModuleHeimdall)
	}
	return nil
}

// Read receive data to from internal connection
func (h *heimdallCli) Read() {
	defer h.Close()
	for {
		records, err := ReadData(h.dataSocketOut)
		if err != nil {
			h.logger.Error(err.Error())
			return
		}
		for _, record := range records {
			if err := checkRecordType(record); err != nil {
				h.logger.Error(err.Error())
				continue
			}
			h.respChan <- record
		}
	}
}

// Close mark itself as not alive and close connection
func (h *heimdallCli) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.alive {
		return
	}
	h.alive = false
	if err := h.dataSocketIn.Close(); err != nil {
		h.logger.Error(err.Error())
	}
	atomic.AddInt32(h.cnt, -1)
	h.logger.Info("close heimdallCli")
}

func checkRecordType(rec array.Record) *errno.Error {
	msgType, err := GetMetaValueFromRecord(rec, string(MessageType))
	if err != nil {
		return errno.NewError(errno.UnknownDataMessage)
	}
	switch msgType {
	case string(DATA):
		return nil
	default:
		return errno.NewError(errno.UnknownDataMessageType, DATA, msgType)
	}
}
