package executor

import (
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
)

const maxRow = 10000

type WideReduce func(input []Chunk, out Chunk, p ...interface{}) error

type WideIterator struct {
	isErrHappend bool
	fn           WideReduce
	rowCnt       int
	dType        influxql.DataType
	chunkCache   []Chunk
	params       []interface{}
}

func NewWideIterator(fn WideReduce, params ...interface{}) *WideIterator {
	r := &WideIterator{
		fn:           fn,
		params:       params,
		chunkCache:   []Chunk{},
		isErrHappend: false,
		dType:        influxql.Unknown,
	}
	return r
}

func (r *WideIterator) Next(ie *IteratorEndpoint, p *IteratorParams) {
	if r.isErrHappend {
		p.err = nil
		return
	}

	inChunk, outChunk := ie.InputPoint.Chunk, ie.OutputPoint.Chunk
	if len(inChunk.Columns()) > 1 {
		p.err = errno.NewError(errno.OnlySupportSingleField)
		r.isErrHappend = true
		return
	}

	colDtype := inChunk.Columns()[0].DataType()
	if r.dType == influxql.Unknown {
		r.dType = colDtype
	} else if r.dType != colDtype {
		p.err = errno.NewError(errno.DtypeNotMatch, r.dType, colDtype)
		r.isErrHappend = true
		return
	}

	r.rowCnt += inChunk.NumberOfRows()
	if r.rowCnt > maxRow {
		p.err = errno.NewError(errno.DataTooMuch, maxRow, r.rowCnt)
		r.isErrHappend = true
		return
	}

	r.chunkCache = append(r.chunkCache, inChunk.Clone())
	if !p.lastChunk {
		return
	}
	err := r.fn(r.chunkCache, outChunk, r.params...)
	if err != nil {
		p.err = err
	}
}
