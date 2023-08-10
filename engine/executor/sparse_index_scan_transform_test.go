/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

package executor_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/stretchr/testify/assert"
)

type MocTsspFile struct {
	path string
	immutable.TSSPFile
}

func (m MocTsspFile) Path() string {
	return m.path
}

func buildInputRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val0", Type: influxql.Float},
	)
	return rowDataType
}

func buildInputChunk(name string) executor.Chunk {
	rowDataType := buildInputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk(name)
	chunk.AppendTime([]int64{1, 2, 3}...)
	chunk.AddTagAndIndex(*ParseChunkTags("tag1=" + "tag1val"), 0)
	chunk.AddIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{1, 2, 3}...)
	chunk.Column(0).AppendColumnTimes([]int64{1, 2, 3}...)
	chunk.Column(0).AppendManyNotNil(3)
	return chunk
}

func buildInputSchema() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
	}
	opt.Dimensions = append(opt.Dimensions, "tag1")
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

type MockStoreEngine struct {
}

func NewMockStoreEngine() *MockStoreEngine {
	return &MockStoreEngine{}
}

func (s *MockStoreEngine) ReportLoad() {
}

func (s *MockStoreEngine) CreateLogicPlan(_ context.Context, _ string, _ uint32, _ uint64, _ influxql.Sources, _ hybridqp.Catalog) (hybridqp.QueryNode, error) {
	return nil, nil
}

func (s *MockStoreEngine) ScanWithSparseIndex(_ context.Context, _ string, _ uint32, _ []uint64, _ hybridqp.Catalog) (hybridqp.IShardsFragments, error) {
	return buildShardsFragments1(), nil
}

func (s *MockStoreEngine) UnrefEngineDbPt(_ string, _ uint32) {

}

func (s *MockStoreEngine) GetShardDownSampleLevel(_ string, _ uint32, _ uint64) int {
	return 0
}

func buildIndexScanExtraInfo() *executor.IndexScanExtraInfo {
	info := &executor.IndexScanExtraInfo{
		Store: NewMockStoreEngine(),
		Req: &executor.RemoteQuery{
			Database: "db0",
			PtID:     uint32(0),
			ShardIDs: []uint64{1, 2, 3},
		},
	}
	return info
}

func getTSSPFiles(fileNum int) []immutable.TSSPFile {
	var files []immutable.TSSPFile
	lockPath := "./tssp"
	for i := 0; i < fileNum; i++ {
		fileName := fmt.Sprintf("00000001-0001-0000000%d.tssp", i)
		file := &MocTsspFile{path: lockPath + "/" + fileName}
		files = append(files, file)
	}
	return files
}

func buildShardsFragments1() executor.ShardsFragments {
	files := getTSSPFiles(3)
	f1, f2, f3 := files[0], files[1], files[2]
	shardsFragments := map[uint64]*executor.FileFragments{
		1: {FragmentCount: 15,
			FileMarks: map[string]executor.FileFragment{
				f1.Path(): executor.NewFileFragment(f1, fragment.FragmentRanges{{Start: 1, End: 10}, {Start: 14, End: 20}}, 15)}},
		2: {FragmentCount: 51,
			FileMarks: map[string]executor.FileFragment{
				f2.Path(): executor.NewFileFragment(f2, fragment.FragmentRanges{{Start: 5, End: 10}, {Start: 14, End: 60}}, 51)}},
		3: {FragmentCount: 11,
			FileMarks: map[string]executor.FileFragment{
				f3.Path(): executor.NewFileFragment(f3, fragment.FragmentRanges{{Start: 1, End: 4}, {Start: 12, End: 20}}, 11)}},
	}
	return shardsFragments
}

func buildShardsFragmentsGroups11() *executor.ShardsFragmentsGroups {
	var groups []*executor.ShardsFragmentsGroup
	groups = append(groups, executor.NewShardsFragmentsGroup(buildShardsFragments1(), 77))
	return &executor.ShardsFragmentsGroups{Items: groups}
}

func buildShardsFragmentsGroups12() *executor.ShardsFragmentsGroups {
	var groups []*executor.ShardsFragmentsGroup
	files := getTSSPFiles(3)
	f1, f2, f3 := files[0], files[1], files[2]
	frags := map[uint64]*executor.FileFragments{
		1: {FragmentCount: 15,
			FileMarks: map[string]executor.FileFragment{
				f1.Path(): executor.NewFileFragment(f1, fragment.FragmentRanges{{Start: 1, End: 10}, {Start: 14, End: 20}}, 15)}},
		2: {FragmentCount: 12,
			FileMarks: map[string]executor.FileFragment{
				f2.Path(): executor.NewFileFragment(f2, fragment.FragmentRanges{{Start: 5, End: 10}, {Start: 14, End: 21}}, 12)}},
		3: {FragmentCount: 11,
			FileMarks: map[string]executor.FileFragment{
				f3.Path(): executor.NewFileFragment(f3, fragment.FragmentRanges{{Start: 1, End: 4}, {Start: 12, End: 20}}, 11)}},
	}
	group1 := executor.NewShardsFragmentsGroup(frags, 38)

	frags = map[uint64]*executor.FileFragments{
		2: {FragmentCount: 39,
			FileMarks: map[string]executor.FileFragment{
				f2.Path(): executor.NewFileFragment(f2, fragment.FragmentRanges{{Start: 21, End: 60}}, 39)}},
	}
	group2 := executor.NewShardsFragmentsGroup(frags, 39)
	groups = append(groups, group1, group2)
	return &executor.ShardsFragmentsGroups{Items: groups}
}

func buildShardsFragmentsGroups14() *executor.ShardsFragmentsGroups {
	var groups []*executor.ShardsFragmentsGroup
	files := getTSSPFiles(3)
	f1, f2, f3 := files[0], files[1], files[2]
	frags := map[uint64]*executor.FileFragments{
		2: {FragmentCount: 7,
			FileMarks: map[string]executor.FileFragment{
				f2.Path(): executor.NewFileFragment(f2, fragment.FragmentRanges{{Start: 34, End: 41}}, 7)}},
		3: {FragmentCount: 11,
			FileMarks: map[string]executor.FileFragment{
				f3.Path(): executor.NewFileFragment(f3, fragment.FragmentRanges{{Start: 1, End: 4}, {Start: 12, End: 20}}, 11)}},
	}
	group1 := executor.NewShardsFragmentsGroup(frags, 18)

	frags = map[uint64]*executor.FileFragments{
		2: {FragmentCount: 19,
			FileMarks: map[string]executor.FileFragment{
				f2.Path(): executor.NewFileFragment(f2, fragment.FragmentRanges{{Start: 41, End: 60}}, 19)}},
	}
	group2 := executor.NewShardsFragmentsGroup(frags, 19)

	frags = map[uint64]*executor.FileFragments{
		1: {FragmentCount: 15,
			FileMarks: map[string]executor.FileFragment{
				f1.Path(): executor.NewFileFragment(f1, fragment.FragmentRanges{{Start: 1, End: 10}, {Start: 14, End: 20}}, 15)}},
		2: {FragmentCount: 5,
			FileMarks: map[string]executor.FileFragment{
				f2.Path(): executor.NewFileFragment(f2, fragment.FragmentRanges{{Start: 5, End: 10}}, 5)}},
	}
	group3 := executor.NewShardsFragmentsGroup(frags, 20)

	frags = map[uint64]*executor.FileFragments{
		2: {FragmentCount: 20,
			FileMarks: map[string]executor.FileFragment{
				f2.Path(): executor.NewFileFragment(f2, fragment.FragmentRanges{{Start: 14, End: 34}}, 20)}},
	}
	group4 := executor.NewShardsFragmentsGroup(frags, 20)
	groups = append(groups, group1, group2, group3, group4)
	return &executor.ShardsFragmentsGroups{Items: groups}
}

func buildShardsFragments2() executor.ShardsFragments {
	files := getTSSPFiles(2)
	f1, f2 := files[0], files[1]
	shardsFragments := map[uint64]*executor.FileFragments{
		1: {FragmentCount: 66,
			FileMarks: map[string]executor.FileFragment{
				f1.Path(): executor.NewFileFragment(f1, fragment.FragmentRanges{{Start: 1, End: 10}, {Start: 14, End: 20}}, 15),
				f2.Path(): executor.NewFileFragment(f2, fragment.FragmentRanges{{Start: 5, End: 10}, {Start: 14, End: 60}}, 51)},
		},
	}
	return shardsFragments
}

func buildShardsFragmentsGroups21() *executor.ShardsFragmentsGroups {
	var groups []*executor.ShardsFragmentsGroup
	groups = append(groups, executor.NewShardsFragmentsGroup(buildShardsFragments2(), 66))
	return &executor.ShardsFragmentsGroups{Items: groups}
}

func buildShardsFragmentsGroups22() *executor.ShardsFragmentsGroups {
	var groups []*executor.ShardsFragmentsGroup
	files := getTSSPFiles(2)
	f1, f2 := files[0], files[1]
	frags := map[uint64]*executor.FileFragments{
		1: {FragmentCount: 33,
			FileMarks: map[string]executor.FileFragment{
				f1.Path(): executor.NewFileFragment(f1, fragment.FragmentRanges{{Start: 1, End: 10}, {Start: 14, End: 20}}, 15),
				f2.Path(): executor.NewFileFragment(f2, fragment.FragmentRanges{{Start: 5, End: 10}, {Start: 14, End: 27}}, 18)},
		},
	}
	group1 := executor.NewShardsFragmentsGroup(frags, 33)

	frags = executor.NewShardsFragments()
	fileFrags := executor.NewFileFragments()
	fileFrags.AddFileFragment(f2.Path(), executor.NewFileFragment(f2, fragment.FragmentRanges{{Start: 27, End: 60}}, 33), 33)
	frags[1] = fileFrags
	group2 := executor.NewShardsFragmentsGroup(frags, 33)

	groups = append(groups, group1, group2)
	return &executor.ShardsFragmentsGroups{Items: groups}
}

func buildShardsFragmentsGroups24() *executor.ShardsFragmentsGroups {
	var groups []*executor.ShardsFragmentsGroup
	files := getTSSPFiles(2)
	f1, f2 := files[0], files[1]
	frags := map[uint64]*executor.FileFragments{
		1: {FragmentCount: 16,
			FileMarks: map[string]executor.FileFragment{
				f1.Path(): executor.NewFileFragment(f1, fragment.FragmentRanges{{Start: 8, End: 10}, {Start: 14, End: 20}}, 8),
				f2.Path(): executor.NewFileFragment(f2, fragment.FragmentRanges{{Start: 5, End: 10}, {Start: 14, End: 17}}, 8)},
		},
	}
	group1 := executor.NewShardsFragmentsGroup(frags, 16)

	frags = map[uint64]*executor.FileFragments{
		1: {FragmentCount: 16,
			FileMarks: map[string]executor.FileFragment{
				f1.Path(): executor.NewFileFragment(f1, fragment.FragmentRanges{{Start: 1, End: 8}}, 7),
				f2.Path(): executor.NewFileFragment(f2, fragment.FragmentRanges{{Start: 34, End: 43}}, 9)},
		},
	}
	group2 := executor.NewShardsFragmentsGroup(frags, 16)

	frags = map[uint64]*executor.FileFragments{
		1: {FragmentCount: 17,
			FileMarks: map[string]executor.FileFragment{
				f2.Path(): executor.NewFileFragment(f2, fragment.FragmentRanges{{Start: 43, End: 60}}, 17)},
		},
	}
	group3 := executor.NewShardsFragmentsGroup(frags, 17)

	frags = map[uint64]*executor.FileFragments{
		1: {FragmentCount: 17,
			FileMarks: map[string]executor.FileFragment{
				f2.Path(): executor.NewFileFragment(f2, fragment.FragmentRanges{{Start: 17, End: 34}}, 17)},
		},
	}
	group4 := executor.NewShardsFragmentsGroup(frags, 17)

	groups = append(groups, group1, group2, group3, group4)
	return &executor.ShardsFragmentsGroups{Items: groups}
}

func testEqualShardsFragmentsGroups(t *testing.T, expected, actual *executor.ShardsFragmentsGroups) {
	if expected.Len() != actual.Len() {
		t.Fatal("the num of the expected ShardsFragmentsGroup is different from actual")
	}
	for i := range expected.Items {
		es := expected.Items[i].GetFrags()
		as := actual.Items[i].GetFrags()
		for shardId, shardFrags := range es {
			if shardFrags1, ok := as[shardId]; ok {
				assert.Equal(t, shardFrags.FragmentCount, shardFrags1.FragmentCount)
				for filePath, fileFragment := range shardFrags.FileMarks {
					if fileFragment1, ok := shardFrags1.FileMarks[filePath]; ok {
						assert.Equal(t, fileFragment, fileFragment1)
					} else {
						t.Fatal("the num of the expected FileFragment is different from actual")
					}
				}
			} else {
				t.Fatal("the num of the expected FileFragments is different from actual")
			}
		}
		assert.Equal(t, expected.Items[i].GetFragmentCount(), actual.Items[i].GetFragmentCount())
	}
}

func TestDistributeFragments(t *testing.T) {
	var err error
	var expected, actual *executor.ShardsFragmentsGroups
	t.Run("1-1", func(t *testing.T) {
		frags := buildShardsFragments1()
		expected = buildShardsFragmentsGroups11()
		actual, err = executor.DistributeFragments(frags, 1)
		if err != nil {
			t.Fatal(err)
		}
		testEqualShardsFragmentsGroups(t, expected, actual)
	})

	t.Run("1-2", func(t *testing.T) {
		frags := buildShardsFragments1()
		expected = buildShardsFragmentsGroups12()
		actual, err = executor.DistributeFragments(frags, 2)
		if err != nil {
			t.Fatal(err)
		}
		testEqualShardsFragmentsGroups(t, expected, actual)
	})

	t.Run("1-4", func(t *testing.T) {
		frags := buildShardsFragments1()
		expected = buildShardsFragmentsGroups14()
		actual, err = executor.DistributeFragments(frags, 4)
		if err != nil {
			t.Fatal(err)
		}
		testEqualShardsFragmentsGroups(t, expected, actual)
	})

	t.Run("2-1", func(t *testing.T) {
		frags := buildShardsFragments2()
		expected = buildShardsFragmentsGroups21()
		actual, err = executor.DistributeFragments(frags, 1)
		if err != nil {
			t.Fatal(err)
		}
		testEqualShardsFragmentsGroups(t, expected, actual)
	})

	t.Run("2-2", func(t *testing.T) {
		frags := buildShardsFragments2()
		expected = buildShardsFragmentsGroups22()
		actual, err = executor.DistributeFragments(frags, 2)
		if err != nil {
			t.Fatal(err)
		}
		testEqualShardsFragmentsGroups(t, expected, actual)
	})

	t.Run("2-4", func(t *testing.T) {
		frags := buildShardsFragments2()
		expected = buildShardsFragmentsGroups24()
		actual, err = executor.DistributeFragments(frags, 4)
		if err != nil {
			t.Fatal(err)
		}
		testEqualShardsFragmentsGroups(t, expected, actual)
	})
}

func TestSparseIndexScanTransform(t *testing.T) {
	// init the chunk and schema
	ctx := context.Background()
	chunk1 := buildInputChunk("m")
	outputRowDataType := buildInputRowDataType()
	schema := buildInputSchema()
	info := buildIndexScanExtraInfo()

	// build the logical plan
	series := executor.NewLogicalMst(outputRowDataType)
	agg := executor.NewLogicalAggregate(series, schema)
	index := executor.NewLogicalSparseIndexScan(agg, schema)

	// build the pipeline executor
	var process []executor.Processor
	trans := executor.NewSparseIndexScanTransform(outputRowDataType, index.Children()[0], index.RowExprOptions(), info, index.Schema())
	source := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	sink := NewSinkFromFunction(outputRowDataType, func(chunk executor.Chunk) error {
		return nil
	})
	if err := executor.Connect(source.GetOutputs()[0], trans.GetInputs()[0]); err != nil {
		t.Fatal(err)
	}
	if err := executor.Connect(trans.GetOutputs()[0], sink.Input); err != nil {
		t.Fatal(err)
	}
	process = append(process, source)
	process = append(process, trans)
	process = append(process, sink)
	exec := executor.NewPipelineExecutor(process)

	// build the sub pipeline executor
	var childProcess []executor.Processor
	childSource := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	childSink := NewSinkFromFunction(outputRowDataType, func(chunk executor.Chunk) error {
		return nil
	})
	childProcess = append(childProcess, childSource)
	childProcess = append(childProcess, childSink)
	childExec := executor.NewPipelineExecutor(childProcess)
	childDag := executor.NewTransformDag()
	root := executor.NewTransformVertex(index, childSink)
	childDag.AddVertex(root)
	childExec.SetRoot(root)
	childExec.SetDag(childDag)

	// make the pipeline executor work
	assert.Equal(t, trans.Name(), "SparseIndexScanTransform")
	assert.Equal(t, len(trans.Explain()), 1)
	assert.Equal(t, trans.GetInputNumber(executor.NewChunkPort(outputRowDataType)), 0)
	assert.Equal(t, trans.GetOutputNumber(executor.NewChunkPort(outputRowDataType)), 0)

	go func() {
		if err := trans.Work(ctx); err != nil {
			t.Error(err)
			return
		}
	}()

	exec.Release()
	childExec.Release()
	trans.Close()

	// error check
	scan := executor.NewSparseIndexScanTransform(outputRowDataType, index.Children()[0], index.RowExprOptions(), nil, index.Schema())
	err := scan.Work(ctx)
	if err != nil {
		assert.Equal(t, err, errors.New("SparseIndexScanTransform get the info failed"))
	}
	cancelCtx, cancelFunc := context.WithCancel(ctx)
	scan = executor.NewSparseIndexScanTransform(outputRowDataType, index.Children()[0], index.RowExprOptions(), info, index.Schema())
	go scan.Running(cancelCtx)
	time.Sleep(1 * time.Second)
	cancelFunc()
	go scan.Running(cancelCtx)
	time.Sleep(1 * time.Second)
	trans.GetInputs().Close()
	fileFrag := executor.NewFileFragment(getTSSPFiles(1)[0], fragment.FragmentRanges{{Start: 27, End: 60}}, 33)
	newFileFrag := fileFrag.CutTo(44)
	assert.Equal(t, fileFrag, newFileFrag)
}
