package executor

import (
	"github.com/openGemini/openGemini/open_src/influx/influxql"
)

func HeimdallDetectReduce(in, out Chunk, _ ...interface{}) error {
	out.SetName(in.Name())
	out.AppendTime(in.Time()...)
	out.AppendIntervalIndex(in.IntervalIndex()...)
	out.AppendTagsAndIndexes(in.Tags(), in.TagIndex())
	for i, col := range in.Columns() {
		dataType := col.DataType()
		switch dataType {
		case influxql.Integer:
			out.Column(i).AppendIntegerValues(in.Column(i).IntegerValues()...)
			in.Column(i).NilsV2().CopyTo(out.Column(i).NilsV2())
		case influxql.Float:
			out.Column(i).AppendFloatValues(in.Column(i).FloatValues()...)
			in.Column(i).NilsV2().CopyTo(out.Column(i).NilsV2())
		}
	}
	return nil
}
