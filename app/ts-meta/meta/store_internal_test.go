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

package meta

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/metaclient"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/stretchr/testify/require"
)

func TestCheckLeaderChanged(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()
	s := mms.GetStore()
	s.notifyCh <- false
	time.Sleep(time.Second)
	select {
	case <-s.stepDown:

	default:
		t.Fatal(fmt.Errorf("leader should step down"))
	}
}

func Test_getSnapshot(t *testing.T) {
	s := &Store{
		cacheData: &meta2.Data{
			Term:         1,
			Index:        2,
			ClusterID:    3,
			ClusterPtNum: 4,
			PtNumPerNode: 5,

			Databases: map[string]*meta2.DatabaseInfo{
				"db0": {
					Name: "db0",
					RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
						"rp0": {
							Measurements: map[string]*meta2.MeasurementInfo{
								"cpu-1": {
									Name: "cpu-1",
								},
							},
						},
					},
				},
			},
			Users: []meta2.UserInfo{
				{
					Name: "test",
				},
			},
		},
		cacheDataBytes: []byte{1, 2, 3},
	}

	// case sql
	sqlBytes := s.getSnapshot(metaclient.SQL)
	require.Equal(t, []byte{1, 2, 3}, sqlBytes)

	// case store
	storeBytes := s.getSnapshot(metaclient.STORE)
	data := &meta2.Data{}
	require.NoError(t, data.UnmarshalBinary(storeBytes))
	require.Equal(t, len(data.Databases), len(s.cacheData.Databases))
	require.Equal(t, len(data.Users), len(s.cacheData.Users))

	// case meta
	metaBytes := s.getSnapshot(metaclient.META)
	require.Equal(t, []byte{1, 2, 3}, metaBytes)
}

type MockRaft struct {
	isLeader bool
}

func (m MockRaft) State() raft.RaftState {
	panic("implement me")
}

func (m MockRaft) Peers() ([]string, error) {
	panic("implement me")
}

func (m MockRaft) Close() error {
	panic("implement me")
}

func (m MockRaft) IsLeader() bool {
	return m.isLeader
}

func (m MockRaft) IsCandidate() bool {
	panic("implement me")
}

func (m MockRaft) Leader() string {
	panic("implement me")
}

func (m MockRaft) Apply(b []byte) error {
	panic("implement me")
}

func (m MockRaft) AddServer(addr string) error {
	panic("implement me")
}

func (m MockRaft) ShowDebugInfo(witch string) ([]byte, error) {
	panic("implement me")
}

func (m MockRaft) UserSnapshot() error {
	panic("implement me")
}

func (m MockRaft) LeadershipTransfer() error {
	panic("implement me")
}

func Test_GetStreamInfo(t *testing.T) {
	Streams := map[string]*meta2.StreamInfo{}
	Streams["test"] = &meta2.StreamInfo{
		Name:     "test",
		ID:       0,
		SrcMst:   &meta2.StreamMeasurementInfo{},
		DesMst:   &meta2.StreamMeasurementInfo{},
		Interval: 10,
		Dims:     []string{"test"},
		Calls:    nil,
		Delay:    0,
	}
	raft := &MockRaft{}
	s := &Store{
		cacheData: &meta2.Data{
			Term:         1,
			Index:        2,
			ClusterID:    3,
			ClusterPtNum: 4,
			PtNumPerNode: 5,
			Streams:      Streams,
		},
		cacheDataBytes: []byte{1, 2, 3},
		raft:           raft,
	}
	_, err := s.getStreamInfo()
	if err == nil {
		t.Fatal("node is not the leader, cannot get info")
	}
	raft.isLeader = true
	_, err = s.getStreamInfo()
	if err != nil {
		t.Fatal(err)
	}
}

func Test_MeasurementInfo(t *testing.T) {
	raft := &MockRaft{}
	s := &Store{
		cacheData: &meta2.Data{
			Term:         1,
			Index:        2,
			ClusterID:    3,
			ClusterPtNum: 4,
			PtNumPerNode: 5,
			Databases: map[string]*meta2.DatabaseInfo{
				"db0": {
					Name: "db0",
					RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
						"rp0": {
							Measurements: map[string]*meta2.MeasurementInfo{
								"cpu-1_0000": {
									Name: "cpu-1",
								},
							},

							MstVersions: map[string]meta2.MeasurementVer{
								"cpu-1": {
									NameWithVersion: "cpu-1_0000",
									Version:         0,
								},
							},
						},
					},
				},
			},
		},
		cacheDataBytes: []byte{1, 2, 3},
		raft:           raft,
	}
	_, err := s.getMeasurementInfo("test", "test", "test")
	if err == nil {
		t.Fatal("node is not the leader, cannot get info")
	}
	raft.isLeader = true
	_, err = s.getMeasurementInfo("test", "test", "test")
	if err == nil {
		t.Fatal("db not find")
	}
	_, err = s.getMeasurementInfo("db0", "rp0", "cpu-1")
	if err != nil {
		t.Fatal(err)
	}
}

func Test_MeasurementsInfo(t *testing.T) {
	raft := &MockRaft{}
	s := &Store{
		cacheData: &meta2.Data{
			Term:         1,
			Index:        2,
			ClusterID:    3,
			ClusterPtNum: 4,
			PtNumPerNode: 5,
			Databases: map[string]*meta2.DatabaseInfo{
				"db0": {
					Name: "db0",
					RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
						"rp0": {
							Measurements: map[string]*meta2.MeasurementInfo{
								"cpu-1_0000": {
									Name:       "cpu-1",
									EngineType: config.COLUMNSTORE,
								},
							},
						},
					},
				},
			},
		},
		cacheDataBytes: []byte{1, 2, 3},
		raft:           raft,
	}
	_, err := s.getMeasurementsInfo("test", "test")
	if err == nil {
		t.Fatal("node is not the leader, cannot get info")
	}
	raft.isLeader = true
	_, err = s.getMeasurementsInfo("test", "test")
	if err == nil {
		t.Fatal("db not find")
	}
	_, err = s.getMeasurementsInfo("db0", "rp0")
	if err != nil {
		t.Fatal(err)
	}
}

func Test_GetDownSampleInfo(t *testing.T) {
	r := &MockRaft{}
	s := &Store{
		raft: r,
	}
	g := &GetDownSampleInfo{
		BaseHandler{
			store:   s,
			closing: make(chan struct{}),
		},
		&message.GetDownSampleInfoRequest{},
	}
	rsp, _ := g.Process()
	if rsp.(*message.GetDownSampleInfoResponse).Err != raft.ErrNotLeader.Error() {
		t.Fatal("unexpected error")
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		g.closing <- struct{}{}
		wg.Done()
	}()
	time.Sleep(time.Second * 2)
	rsp2, _ := g.Process()
	wg.Wait()
	if rsp2.(*message.GetDownSampleInfoResponse).Err != "server closed" {
		t.Fatal("unexpected error")
	}
}

func Test_GetRpMstInfos(t *testing.T) {
	r := &MockRaft{}
	s := &Store{
		raft: r,
	}
	g := &GetRpMstInfos{
		BaseHandler{
			store:   s,
			closing: make(chan struct{}),
		},
		&message.GetRpMstInfosRequest{},
	}
	rsp, _ := g.Process()
	if rsp.(*message.GetRpMstInfosResponse).Err != raft.ErrNotLeader.Error() {
		t.Fatal("unexpected error")
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		g.closing <- struct{}{}
		wg.Done()
	}()
	time.Sleep(time.Second * 2)
	rsp2, _ := g.Process()
	wg.Wait()
	if rsp2.(*message.GetRpMstInfosResponse).Err != "server closed" {
		t.Fatal("unexpected error")
	}
}

func TestGetReplicaInfo(t *testing.T) {
	store := &Store{}
	data := &meta2.Data{
		PtView:        make(map[string]meta2.DBPtInfos),
		ReplicaGroups: make(map[string][]meta2.ReplicaGroup),
	}
	store.data = data
	store.raft = &MockRaft{isLeader: true}

	data.PtView["db0"] = append(data.PtView["db0"], meta2.PtInfo{
		Owner: meta2.PtOwner{NodeID: 1},
		PtId:  1,
		RGID:  1,
	}, meta2.PtInfo{
		Owner: meta2.PtOwner{NodeID: 2},
		PtId:  2,
		RGID:  1,
	})
	data.ReplicaGroups["db0"] = append(data.ReplicaGroups["db0"], meta2.ReplicaGroup{
		ID:         1,
		MasterPtID: 1,
		Peers:      []meta2.Peer{{ID: 2, PtRole: meta2.Slave}},
	}, meta2.ReplicaGroup{
		ID:         2,
		MasterPtID: 3,
		Peers:      []meta2.Peer{},
	})

	// master
	info, err := store.GetReplicaInfo("db0", 1, 1)
	require.NoError(t, err)
	require.Equal(t, meta2.Master, info.ReplicaRole)
	require.Equal(t, 1, len(info.Peers))
	require.Equal(t, uint32(2), info.Peers[0].PtId)

	// slave
	info, err = store.GetReplicaInfo("db0", 2, 2)
	require.NoError(t, err)
	require.Equal(t, meta2.Slave, info.ReplicaRole)
	require.Equal(t, uint32(1), info.Master.PtId)

	_, err = store.GetReplicaInfo("db_not_exists", 2, 2)
	require.NotEmpty(t, err)

	store.raft = &MockRaft{isLeader: false}
	_, err = store.GetReplicaInfo("db_not_exists", 2, 2)
	require.EqualError(t, err, raft.ErrNotLeader.Error())
}
