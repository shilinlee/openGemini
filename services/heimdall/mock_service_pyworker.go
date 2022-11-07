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
	"net"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

func mockPyworkerHandleData(recs []array.Record, conn net.Conn) error {
	for _, rec := range recs {
		metaKeys := rec.Schema().Metadata().Keys()
		metaVals := rec.Schema().Metadata().Values()
		var newKeys []string
		var newVals []string
		for i := 0; i < len(metaKeys); i++ {
			newKeys = append(newKeys, metaKeys[i])
			newVals = append(newVals, metaVals[i])
		}
		newKeys = append(newKeys, string(AnomalyNum))
		newVals = append(newVals, "1")

		fields := []arrow.Field{
			{Name: string(AnomalyLevel), Type: arrow.PrimitiveTypes.Float64},
			{Name: string(DataTime), Type: arrow.PrimitiveTypes.Int64},
		}
		newMeta := arrow.NewMetadata(newKeys, newVals)
		schema := arrow.NewSchema(fields, &newMeta)

		pool := memory.NewGoAllocator()
		b := array.NewRecordBuilder(pool, schema)
		defer b.Release()

		// copy input chunk values
		valid := []bool{true}
		b.Field(0).(*array.Float64Builder).AppendValues([]float64{0}, valid)
		b.Field(1).(*array.Int64Builder).AppendValues([]int64{0}, valid)
		if err := WriteData(b.NewRecord(), conn); err != nil {
			return err
		}
	}
	return nil
}

// mock pyworker to handle data
func MockPyWorker(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if err = listener.Close(); err != nil {
			return
		}
		connType := make([]byte, len(BATCH))
		for string(connType) != string(BATCH) {
			if _, err = conn.Read(connType); err != nil {
				return
			}
		}
		for {
			readerBuf := bufio.NewReader(conn)
			recs, err := ReadData(readerBuf)
			if err != nil {
				return
			}
			if err := mockPyworkerHandleData(recs, conn); err != nil {
				return
			}
		}
	}()
	return nil
}

// mock heimdall response
func BuildNumericRecord() array.Record {
	metaKeys := []string{"t", string(AnomalyNum), string(MessageType), string(TaskID)}
	metaVals := []string{"1", "1", string(DATA), ""}
	metaData := arrow.NewMetadata(metaKeys, metaVals)

	fields := []arrow.Field{
		{Name: "int", Type: arrow.PrimitiveTypes.Int64},
		{Name: string(AnomalyLevel), Type: arrow.PrimitiveTypes.Float64},
		{Name: string(DataTime), Type: arrow.PrimitiveTypes.Int64}, // timestamp must store at last column
	}

	schema := arrow.NewSchema(fields, &metaData)
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	valid := []bool{true, true, true, true}
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{0, 1, 2, 3}, valid)
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{0, 1.0, 2.0, 3.0}, valid)
	b.Field(2).(*array.Int64Builder).AppendValues([]int64{0, 1, 2, 3}, valid)

	rec := b.NewRecord()
	return rec
}
