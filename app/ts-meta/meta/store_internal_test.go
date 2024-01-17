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
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	assert2 "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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
		cacheDataBytes:   []byte{1, 2, 3},
		cacheDataChanged: make(chan struct{}),
	}

	// case sql
	sqlBytes := s.getSnapshot(metaclient.SQL)
	require.Equal(t, []byte{1, 2, 3}, sqlBytes)

	// case meta
	metaBytes := s.getSnapshot(metaclient.META)
	require.Equal(t, []byte{1, 2, 3}, metaBytes)

	// case store
	s.data = s.cacheData
	s.updateCacheData()
	storeBytes := s.getSnapshot(metaclient.STORE)
	data := &meta2.Data{}
	require.NoError(t, data.UnmarshalBinary(storeBytes))
	require.Equal(t, len(data.Databases), len(s.cacheData.Databases))
	require.Equal(t, len(data.Users), len(s.cacheData.Users))
}

type MockRaft struct {
	isLeader bool

	RaftInterface
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

func Test_applyDropDatabaseCommand(t *testing.T) {
	fsm := &storeFSM{
		data: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{
				"db0": {
					ContinuousQueries: map[string]*meta2.ContinuousQueryInfo{
						"cq0": {},
					},
				},
			},
		},
	}

	// db not exist
	typ := proto2.Command_DropDatabaseCommand
	desc := proto2.E_DropDatabaseCommand_Command
	value := &proto2.DropDatabaseCommand{
		Name: proto.String("db999"),
	}
	cmd := &proto2.Command{Type: &typ}
	_ = proto.SetExtension(cmd, desc, value)
	err := fsm.applyDropDatabaseCommand(cmd)
	assert2.Nil(t, err)

	// drop successfully and notify cq changed
	value = &proto2.DropDatabaseCommand{
		Name: proto.String("db0"),
	}
	cmd = &proto2.Command{Type: &typ}
	_ = proto.SetExtension(cmd, desc, value)
	err = fsm.applyDropDatabaseCommand(cmd)
	assert2.Nil(t, err)
	assert2.Equal(t, uint64(1), fsm.data.MaxCQChangeID)
	_, ok := fsm.data.Databases["db0"] // db0 is dropped successfully
	assert2.False(t, ok)
}

func Test_applyCreateContinuousQuery(t *testing.T) {
	s := &Store{
		raft:     &MockRaftForCQ{isLeader: true},
		sqlHosts: []string{"127.0.0.1:8086"},
		cqLease: map[string]*cqLeaseInfo{
			"127.0.0.1:8086": {},
		},
		data:   &meta2.Data{},
		Logger: logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()),
	}
	fsm := (*storeFSM)(s)
	value := &proto2.CreateContinuousQueryCommand{
		Database: proto.String("db0"),
		Name:     proto.String("cq0"),
		Query:    proto.String(`CREATE CONTINUOUS QUERY "cq0" ON "db0" RESAMPLE EVERY 2h FOR 30m BEGIN SELECT max("passengers") INTO "max_passengers" FROM "bus_data" GROUP BY time(10m) END`),
	}
	typ := proto2.Command_CreateContinuousQueryCommand
	cmd := &proto2.Command{Type: &typ}
	err := proto.SetExtension(cmd, proto2.E_CreateContinuousQueryCommand_Command, value)
	require.NoError(t, err)

	// database not found
	resErr := applyCreateContinuousQuery(fsm, cmd)
	require.EqualError(t, resErr.(error), "database not found: db0")
	fsm.data = &meta2.Data{
		Databases: map[string]*meta2.DatabaseInfo{
			"db0": {
				Name: "db0",
			},
		},
	}
	resErr = applyCreateContinuousQuery(fsm, cmd)
	resErr = applyCreateContinuousQuery(fsm, cmd) // try to create the same CQ
	require.Nil(t, resErr)
	require.Equal(t, "cq0", fsm.data.Databases["db0"].ContinuousQueries["cq0"].Name)
	require.Equal(t, "cq0", fsm.cqNames[0])
	require.Equal(t, "cq0", fsm.cqLease["127.0.0.1:8086"].CQNames[0])
	require.Equal(t, uint64(1), fsm.data.MaxCQChangeID)

	value = &proto2.CreateContinuousQueryCommand{
		Database: proto.String("db0"),
		Name:     proto.String("cq0"),
		Query:    proto.String(`CREATE CONTINUOUS QUERY "cq0" ON "db0" RESAMPLE EVERY 1h FOR 20m BEGIN SELECT max("passengers") INTO "max_passengers" FROM "bus_data" GROUP BY time(1m) END`),
	}
	_ = proto.SetExtension(cmd, proto2.E_CreateContinuousQueryCommand_Command, value)
	resErr = applyCreateContinuousQuery(fsm, cmd) // try to create the same name CQ
	require.EqualError(t, resErr.(error), "continuous query name already exists")
}

func Test_applyContinuousQueryReportCommand(t *testing.T) {
	s := &Store{
		raft: &MockRaftForCQ{isLeader: true},
		data: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{
				"db0": {
					Name: "db0",
					ContinuousQueries: map[string]*meta2.ContinuousQueryInfo{
						"cq0": {
							Name:        "cq0",
							Query:       `CREATE CONTINUOUS QUERY "cq0" ON "db0" RESAMPLE EVERY 2h FOR 30m BEGIN SELECT max("passengers") INTO "max_passengers" FROM "bus_data" GROUP BY time(10m) END`,
							LastRunTime: time.Time{},
						},
					},
				},
			},
		},
		Logger: logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()),
	}
	fsm := (*storeFSM)(s)
	ts := time.Now()
	value := &proto2.ContinuousQueryReportCommand{
		CQStates: []*proto2.CQState{
			{
				Name:        proto.String("cq0"),
				LastRunTime: proto.Int64(ts.UnixNano()),
			},
		},
	}
	typ := proto2.Command_ContinuousQueryReportCommand
	cmd := &proto2.Command{Type: &typ}
	err := proto.SetExtension(cmd, proto2.E_ContinuousQueryReportCommand_Command, value)
	require.NoError(t, err)

	resErr := applyContinuousQueryReport(fsm, cmd)
	require.Nil(t, resErr)
	require.Equal(t, ts.UnixNano(), fsm.data.Databases["db0"].ContinuousQueries["cq0"].LastRunTime.UnixNano())
}

func Test_applyDropContinuousQuery(t *testing.T) {
	s := &Store{
		raft: &MockRaftForCQ{isLeader: true},
		data: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{
				"db0": {
					Name: "db0",
					ContinuousQueries: map[string]*meta2.ContinuousQueryInfo{
						"cq0": {
							Name:        "cq0",
							Query:       `CREATE CONTINUOUS QUERY "cq0" ON "db0" RESAMPLE EVERY 2h FOR 30m BEGIN SELECT max("passengers") INTO "max_passengers" FROM "bus_data" GROUP BY time(10m) END`,
							LastRunTime: time.Time{},
						},
					},
				},
			},
		},
		Logger: logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()),
	}
	fsm := (*storeFSM)(s)
	value := &proto2.DropContinuousQueryCommand{
		Name:     proto.String("cq0"),
		Database: proto.String("db0"),
	}
	typ := proto2.Command_DropContinuousQueryCommand
	cmd := &proto2.Command{Type: &typ}
	err := proto.SetExtension(cmd, proto2.E_DropContinuousQueryCommand_Command, value)
	require.NoError(t, err)

	resErr := applyDropContinuousQuery(fsm, cmd)
	require.Nil(t, resErr)
	require.Equal(t, uint64(1), fsm.data.MaxCQChangeID)
	require.Equal(t, 0, len(fsm.data.Databases["db0"].ContinuousQueries))
}

func Test_applyNotifyCQLeaseChanged(t *testing.T) {
	s := &Store{
		raft: &MockRaftForCQ{isLeader: true},
		data: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{
				"db0": {
					Name: "db0",
					ContinuousQueries: map[string]*meta2.ContinuousQueryInfo{
						"cq0": {
							Name:        "cq0",
							Query:       `CREATE CONTINUOUS QUERY "cq0" ON "db0" RESAMPLE EVERY 2h FOR 30m BEGIN SELECT max("passengers") INTO "max_passengers" FROM "bus_data" GROUP BY time(10m) END`,
							LastRunTime: time.Time{},
						},
					},
				},
			},
		},
		Logger: logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()),
	}
	fsm := (*storeFSM)(s)
	value := &proto2.NotifyCQLeaseChangedCommand{}
	typ := proto2.Command_NotifyCQLeaseChangedCommand
	cmd := &proto2.Command{Type: &typ}
	err := proto.SetExtension(cmd, proto2.E_NotifyCQLeaseChangedCommand_Command, value)
	require.NoError(t, err)

	resErr := applyNotifyCQLeaseChanged(fsm, cmd)
	require.Nil(t, resErr)
	require.Equal(t, uint64(1), fsm.data.MaxCQChangeID)

	// the second time to notify
	resErr = applyNotifyCQLeaseChanged(fsm, cmd)
	require.Nil(t, resErr)
	require.Equal(t, uint64(2), fsm.data.MaxCQChangeID)
}

func Test_applyUpdateMeasuremt(t *testing.T) {
	orgOptions := &meta2.Options{Ttl: 1}
	s := &Store{
		raft: &MockRaftForCQ{isLeader: true},
		data: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{
				"db0": {
					Name: "db0",
					RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
						"rp0": {
							Duration: 1,
							Measurements: map[string]*meta2.MeasurementInfo{
								"cpu_0001": {Options: orgOptions},
							},
							MstVersions: map[string]meta2.MeasurementVer{
								"cpu": {NameWithVersion: "cpu_0001", Version: 1},
							},
						},
					},
				},
			},
		},
		Logger: logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()),
	}
	fsm := (*storeFSM)(s)

	options := &meta2.Options{Ttl: 1}
	value := &proto2.UpdateMeasurementCommand{
		Db:      proto.String("db0"),
		Rp:      proto.String("rp0"),
		Mst:     proto.String("cpu"),
		Options: options.Marshal(),
	}
	typ := proto2.Command_UpdateMeasurementCommand
	cmd := &proto2.Command{Type: &typ}
	err := proto.SetExtension(cmd, proto2.E_UpdateMeasurementCommand_Command, value)
	require.NoError(t, err)

	resErr := applyUpdateMeasurement(fsm, cmd)
	require.Nil(t, resErr)
}
