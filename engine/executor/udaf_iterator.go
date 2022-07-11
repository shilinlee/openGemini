package executor

type WideReduce func(input, out Chunk, p ...interface{}) error

type WideIterator struct {
	fn     WideReduce
	params []interface{}
}

func NewWideIterator(fn WideReduce, params ...interface{}) *WideIterator {
	r := &WideIterator{
		fn:     fn,
		params: params,
	}
	return r
}

func (r *WideIterator) Next(ie *IteratorEndpoint, p *IteratorParams) {
	inChunk, outChunk := ie.InputPoint.Chunk, ie.OutputPoint.Chunk
	err := r.fn(inChunk, outChunk, r.params...)
	if err != nil {
		p.err = err
	}
}
