package executor

import (
	"math"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/services/heimdall"
)

func buildNumericChunk() Chunk {
	row := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "cpu", Type: influxql.Float},
		influxql.VarRef{Val: "mem", Type: influxql.Integer},
		influxql.VarRef{Val: "net", Type: influxql.Float},
		influxql.VarRef{Val: "disk", Type: influxql.Integer},
	)
	cb := NewChunkBuilder(row)
	chunk := cb.NewChunk("heimdall")

	timestamp := []int64{1, 2, 3, 4, 5, 6}
	chunk.AppendTime(timestamp...)

	seriesIdx := []int{0, 1, 4, 5}
	chunk.AppendTagsAndIndexes(
		[]ChunkTags{
			*ParseChunkTags("t=1"), *ParseChunkTags("t=2"),
			*ParseChunkTags("t=3"), *ParseChunkTags("t=4"),
		},
		seriesIdx,
	)

	chunk.AppendIntervalIndex(seriesIdx...) // use to store window index(for marking series), left open right close

	chunk.Column(0).AppendFloatValues(1.0, 3.0, 6.0)
	chunk.Column(0).AppendNilsV2(false, true, false, true, false, true)

	chunk.Column(1).AppendIntegerValues(1, 3, 5, 6)
	chunk.Column(1).AppendNilsV2(true, false, true, true, true, false)

	chunk.Column(2).AppendFloatValues(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)
	chunk.Column(2).AppendManyNotNil(len(timestamp))

	chunk.Column(3).AppendIntegerValues(1, 2, 3, 4, 5, 6)
	chunk.Column(3).AppendManyNotNil(len(timestamp)) // must set up bit map

	return chunk
}

func matchContent(c Chunk, recs []array.Record) bool {
	// timestamp store as a column in array.Record

	// one series one record
	if c.TagLen() != len(recs) {
		return false
	}

	// check number of rows and columns
	nRows := 0
	for _, r := range recs {
		nRows += int(r.NumRows())
		if int(r.NumCols())-1 != c.NumberOfCols() {
			return false
		}
	}
	if nRows != c.NumberOfRows() {
		return false
	}

	// check length and nils equality
	for i, cCol := range c.Columns() {
		rNilsCount := 0
		rColLen := 0
		for _, r := range recs {
			name := c.RowDataType().Field(i).Name()
			rFieldIdx := r.Schema().FieldIndices(name)
			if len(rFieldIdx) != 1 {
				return false
			}
			rCol := r.Column(rFieldIdx[0])
			rNilsCount += rCol.NullN()
			rColLen += rCol.Len()
		}
		if rColLen != cCol.Length() {
			return false
		}
		if rNilsCount != cCol.NilCount() {
			return false
		}
	}

	// check timestamp
	if !isTimeEqual(c, recs) {
		return false
	}

	// chunk tags should all in records' metadata
	if !isTagMatch(c, recs) {
		return false
	}

	recVals := make(map[string]interface{})
	for _, r := range recs {
		for i, rCol := range r.Columns() {
			name := r.Schema().Field(i).Name
			vals, exist := recVals[name]
			switch rCol.DataType().ID() {
			case arrow.FLOAT64:
				tmp := rCol.(*array.Float64).Float64Values()
				if !exist {
					vals = make([]float64, 0)
				}
				vals = append(vals.([]float64), tmp...)
				recVals[name] = vals
			case arrow.INT64:
				tmp := rCol.(*array.Int64).Int64Values()
				if !exist {
					vals = make([]int64, 0)
				}
				vals = append(vals.([]int64), tmp...)
				recVals[name] = vals
			default:
				panic("type not support")
			}
		}
	}
	for i, cCol := range c.Columns() {
		name := c.RowDataType().Field(i).Name()
		tmp := recVals[name]

		switch cCol.DataType() {
		case influxql.Float:
			if cCol.NilCount() == 0 {
				if reflect.DeepEqual(tmp, cCol.FloatValues()) {
					continue
				}
				return false
			}
			for j, cVal := range cCol.FloatValues() {
				actualIdx := cCol.GetTimeIndex(j)
				rVals := tmp.([]float64)
				if rVals[actualIdx] != cVal {
					return false
				}
			}
		case influxql.Integer:
			if cCol.NilCount() == 0 {
				if reflect.DeepEqual(tmp, cCol.IntegerValues()) {
					continue
				}
				return false
			}
			for j, cVal := range cCol.IntegerValues() {
				actualIdx := cCol.GetTimeIndex(j)
				rVals := tmp.([]int64)
				if rVals[actualIdx] != cVal {
					return false
				}
			}
		default:
			panic("type not support")
		}
	}

	return true
}

func isTagMatch(c Chunk, recs []array.Record) bool {
	rKeyVals := make(map[string]map[string]struct{})
	for _, r := range recs {
		for i, k := range r.Schema().Metadata().Keys() {
			tmp := rKeyVals[k]
			if tmp == nil {
				tmp = make(map[string]struct{})
			}
			tmp[r.Schema().Metadata().Values()[i]] = struct{}{}
			rKeyVals[k] = tmp
		}
	}

	for _, t := range c.Tags() {
		for k, v := range t.KeyValues() {
			rVals, exist := rKeyVals[k]
			if !exist {
				return false
			}
			_, exist = rVals[v]
			if !exist {
				return false
			}
		}
	}
	return true
}

func isTimeEqual(c Chunk, recs []array.Record) bool {
	cTimes := c.Time()
	var rTimes []int64

	for _, r := range recs {
		times, ok := r.Column(int(r.NumCols()) - 1).(*array.Int64)
		if !ok {
			return false
		}
		rTimes = append(rTimes, times.Int64Values()...)
	}

	return reflect.DeepEqual(cTimes, rTimes)
}

func isFloatEqual(f1 float64, f2 float64) bool {
	eps := 10e-5
	return math.Abs(f1-f2) <= eps
}

func isFloatColumnEqual(cCol Column, rCol *array.Float64) bool {
	cVirtualIdx := 0
	for j, rVal := range rCol.Float64Values() {
		if rCol.IsNull(j) {
			continue
		}
		if rCol.IsNull(j) != cCol.IsNilV2(j) {
			return false
		}
		cVal := cCol.FloatValue(cVirtualIdx)
		if !isFloatEqual(rVal, cVal) {
			return false
		}
		cVirtualIdx++
	}
	return true
}

func isIntegerColumnEqual(cCol Column, rCol *array.Int64) bool {
	cVirtualIdx := 0
	for j, rVal := range rCol.Int64Values() {
		if rCol.IsNull(j) {
			continue
		}
		if rCol.IsNull(j) != cCol.IsNilV2(j) {
			return false
		}
		cVal := cCol.IntegerValue(cVirtualIdx)
		if rVal != cVal {
			return false
		}
		cVirtualIdx++
	}
	return true
}

func Test_chunkToArrowRecord(t *testing.T) {
	c := buildNumericChunk()
	recs, err := chunkToArrowRecords(c, "algo", "xx.conf", "detect", "123")
	if err != nil {
		t.Fatal("convert pure numeric chunk fail")
	}
	if !matchContent(c, recs) {
		t.Fatal("content not match")
	}
}

func Test_arrowRecordToChunk(t *testing.T) {
	rec := heimdall.BuildNumericRecord()
	baseSchema := rec.Schema()
	row, err := buildChunkSchema(baseSchema)
	if err != nil {
		t.Fatal(err)
	}
	cb := NewChunkBuilder(row)
	chunk := cb.NewChunk("heimdall")
	if err = copyArrowRecordToChunk(rec, chunk, nil); err != nil {
		t.Fatal("convert pure numeric record fail")
	}
	if !matchContent(chunk, []array.Record{rec}) {
		t.Fatal("content not match")
	}
}
