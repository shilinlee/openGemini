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

package netstorage_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/stretchr/testify/assert"
)

func TestSeriesKeysRequestMessage(t *testing.T) {
	req := &netstorage.SeriesKeysRequest{}
	req.Db = proto.String("db0")
	req.PtIDs = []uint32{1, 2, 3, 4, 5}
	req.Condition = proto.String("a=1 and b=2")
	req.Measurements = []string{"mst0", "mst1"}

	msg := netstorage.NewDDLMessage(netstorage.SeriesKeysRequestMessage, req)
	buf, err := msg.Marshal(nil)
	if !assert.NoError(t, err) {
		return
	}

	msg2 := netstorage.NewDDLMessage(netstorage.SeriesKeysRequestMessage, &netstorage.SeriesKeysRequest{})
	if !assert.NoError(t, msg2.Unmarshal(buf)) {
		return
	}

	other, ok := msg2.Data.(*netstorage.SeriesKeysRequest)
	if !assert.Equal(t, ok, true, "unmarshal failed") {
		return
	}

	if !reflect.DeepEqual(req.GetPtIDs(), other.GetPtIDs()) ||
		req.GetDb() != other.GetDb() ||
		req.GetCondition() != other.GetCondition() ||
		!reflect.DeepEqual(req.GetMeasurements(), other.GetMeasurements()) {

		t.Fatalf("codec failed; \nexp: %+v \ngot: %+v", req, other)
	}
}

func TestSeriesKeysResponseMessage(t *testing.T) {
	resp := &netstorage.SeriesKeysResponse{}
	resp.Series = []string{"cpu,hostname=127.0.0.1,role=store", "memory,hostname=127.0.0.2,role=sql"}
	resp.Err = proto.String("no errors")

	buf, err := resp.MarshalBinary()
	if !assert.NoError(t, err) {
		return
	}

	other := &netstorage.SeriesKeysResponse{}
	if !assert.NoError(t, other.UnmarshalBinary(buf)) {
		return
	}

	if !reflect.DeepEqual(resp.GetSeries(), other.GetSeries()) ||
		resp.Error().Error() != other.Error().Error() {

		t.Fatalf("codec SeriesKeysResponse failed \nexp: %+v \ngot: %+v",
			resp, other)
	}
}

func makeDeleteRequestMessage(t *testing.T, typ netstorage.DeleteType) (*netstorage.DeleteRequest, *netstorage.DeleteRequest) {
	req := &netstorage.DeleteRequest{}
	req.Database = "db0"
	req.Type = typ
	req.Measurement = "mst_0"
	req.Rp = "test_1"
	req.ShardIds = []uint64{12, 3, 4, 5}

	msg := netstorage.NewDDLMessage(netstorage.DeleteRequestMessage, req)
	buf, err := msg.Marshal(nil)
	if err != nil {
		t.Fatalf("%v", err)
	}

	msg2 := msg.Instance()
	if err := msg2.Unmarshal(buf); err != nil {
		t.Fatalf("%v", err)
	}

	other, ok := msg2.(*netstorage.DDLMessage).Data.(*netstorage.DeleteRequest)
	if !ok {
		t.Fatalf("unmarshal DeleteRequest failed")
	}

	return req, other
}

func TestDeleteRequestMessage(t *testing.T) {
	req, other := makeDeleteRequestMessage(t, netstorage.MeasurementDelete)
	if !assert.Equal(t, req, other) {
		return
	}

	req, other = makeDeleteRequestMessage(t, netstorage.DatabaseDelete)

	assert.Empty(t, other.Measurement, "expected value of Measurement is empty, got: %v", other.Measurement)
	assert.Empty(t, other.ShardIds, "expected value of ShardIds is empty, got: %+v", other.ShardIds)
	assert.Empty(t, other.Rp, "expected value of Rp is empty, got: %+v", other.Rp)
}

func TestShowTagValuesRequest(t *testing.T) {
	req := &netstorage.ShowTagValuesRequest{}
	req.Db = proto.String("db0")

	tagKeys := make(map[string]map[string]struct{})
	tagKeys["cpu"] = map[string]struct{}{
		"hostname": {},
		"role":     {},
	}
	tagKeys["memory"] = map[string]struct{}{
		"hostname": {},
		"max":      {},
	}
	req.SetTagKeys(tagKeys)
	buf, err := req.MarshalBinary()
	if !assert.NoError(t, err) {
		return
	}

	other := &netstorage.ShowTagValuesRequest{}
	if !assert.NoError(t, other.UnmarshalBinary(buf)) {
		return
	}

	got := make(map[string]map[string]struct{})
	for mst, items := range other.GetTagKeysBytes() {
		got[mst] = make(map[string]struct{})
		for _, item := range items {
			got[mst][string(item)] = struct{}{}
		}
	}

	assert.Equal(t, tagKeys, got)
}

func TestShowTagValuesResponse(t *testing.T) {
	resp := &netstorage.ShowTagValuesResponse{}

	set := netstorage.TableTagSets{
		Name: "cpu",
		Values: netstorage.TagSets{
			{
				Key:   "hostname",
				Value: "127.0.0.1",
			},
			{
				Key:   "hostname",
				Value: "127.0.0.2",
			},
			{
				Key:   "role",
				Value: "store",
			},
		},
	}

	sets := netstorage.TablesTagSets{set}
	resp.SetTagValuesSlice(sets)

	buf, err := resp.MarshalBinary()
	if !assert.NoError(t, err) {
		return
	}

	other := &netstorage.ShowTagValuesResponse{}
	if !assert.NoError(t, other.UnmarshalBinary(buf)) {
		return
	}

	assert.Equal(t, sets, other.GetTagValuesSlice())
}

func TestWritePointsRequest(t *testing.T) {
	req := netstorage.NewWritePointsRequest([]byte{1, 2, 3, 4, 5, 6, 7})
	other, ok := assertCodec(t, req, true, false)
	if !ok {
		return
	}

	assert.Equal(t, req.Points(), other.(*netstorage.WritePointsRequest).Points())
}

func TestWritePointsResponse(t *testing.T) {
	req := netstorage.NewWritePointsResponse(1, "ok")

	other, ok := assertCodec(t, req, true, true)
	if !ok {
		return
	}

	assert.Equal(t, req, other.(*netstorage.WritePointsResponse))
}

func TestInvalidDDLMessage(t *testing.T) {
	msg := &netstorage.DDLMessage{}
	err := msg.Unmarshal(nil)
	if !assert.EqualError(t, err, errno.NewError(errno.ShortBufferSize, 0, 0).Error()) {
		return
	}

	buf := []byte{129}
	assert.EqualError(t, msg.Unmarshal(buf), fmt.Sprintf("unknown message type: %d", buf[0]))
}

func TestSysCtrlRequest(t *testing.T) {
	req := &netstorage.SysCtrlRequest{}
	req.SetMod("test")
	req.SetParam(map[string]string{"sex": "male", "country": "cn"})

	obj, ok := assertCodec(t, req, false, false)
	if !ok {
		return
	}
	other := obj.(*netstorage.SysCtrlRequest)

	assert.Equal(t, req.Mod(), other.Mod())
	assert.Equal(t, req.Param(), other.Param())
}

func TestSysCtrlResponse(t *testing.T) {
	req := &netstorage.SysCtrlResponse{}
	req.SetResult(map[string]string{"sex": "male", "country": "cn"})
	req.SetErr("some error")

	obj, ok := assertCodec(t, req, false, false)
	if !ok {
		return
	}
	other := obj.(*netstorage.SysCtrlResponse)

	assert.Equal(t, req.Result(), other.Result())
	assert.Equal(t, req.Error(), other.Error())
}

func assertCodec(t *testing.T, obj transport.Codec, assertSize bool, assertUnmarshalNil bool) (transport.Codec, bool) {
	buf, err := obj.Marshal(nil)
	if !assert.NoError(t, err) {
		return nil, false
	}

	if assertSize && !assert.Equal(t, len(buf), obj.Size(),
		"invalid size, exp: %d, got: %d", len(buf), obj.Size()) {
		return nil, false
	}

	other := obj.Instance()
	if assertUnmarshalNil && !assert.EqualError(t, other.Unmarshal(nil), errno.NewError(errno.ShortBufferSize, 0, 0).Error()) {
		return nil, false
	}

	if !assert.NoError(t, other.Unmarshal(buf)) {
		return nil, false
	}

	return other, true
}
