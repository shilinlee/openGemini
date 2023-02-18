module github.com/openGemini/openGemini

go 1.16

require (
	github.com/BurntSushi/toml v0.4.1
	github.com/RoaringBitmap/roaring v0.9.1
	github.com/VictoriaMetrics/VictoriaMetrics v1.67.0
	github.com/VictoriaMetrics/fastcache v1.7.0
	github.com/apache/arrow/go/arrow v0.0.0-20200923215132-ac86123a3f01
	github.com/armon/go-metrics v0.3.10
	github.com/gogo/protobuf v1.3.2
	github.com/golang-jwt/jwt v3.2.1+incompatible
	github.com/golang/snappy v0.0.4
	github.com/hashicorp/memberlist v0.3.1
	github.com/hashicorp/raft v1.3.1
	github.com/hashicorp/serf v0.9.6
	github.com/hpcloud/tail v1.0.1-0.20170707194310-a927b6857fc7
	github.com/influxdata/influxdb v1.9.5
	github.com/influxdata/influxql v1.1.1-0.20210223160523-b6ab99450c93
	github.com/influxdata/tdigest v0.0.2-0.20210216194612-fc98d27c9e8b
	github.com/json-iterator/go v1.1.12
	github.com/klauspost/compress v1.13.6
	github.com/mitchellh/cli v1.1.0
	github.com/mitchellh/copystructure v1.2.0
	github.com/panjf2000/ants/v2 v2.5.0
	github.com/pingcap/failpoint v0.0.0-20200702092429-9f69995143ce
	github.com/prometheus/client_golang v1.12.1
	github.com/prometheus/common v0.32.1
	github.com/prometheus/prometheus v1.8.3
	github.com/ryanuber/columnize v2.1.2+incompatible
	github.com/shirou/gopsutil/v3 v3.22.1
	github.com/spf13/cobra v1.3.0
	github.com/stretchr/testify v1.7.1
	github.com/tinylib/msgp v1.1.7-0.20220719154719-f3635b96e483
	github.com/valyala/fastjson v1.6.3
	github.com/xlab/treeprint v1.1.0
	go.etcd.io/bbolt v1.3.5
	go.uber.org/zap v1.19.1
	golang.org/x/sync v0.0.0-20220722155255-886fb9371eb4
	golang.org/x/sys v0.0.0-20220722155257-8c9f86f7a55f
	golang.org/x/text v0.4.0
	golang.org/x/time v0.0.0-20210723032227-1f47c861a9ac
	google.golang.org/protobuf v1.27.1
	gopkg.in/natefinch/lumberjack.v2 v2.0.1-0.20190411184413-94d9e492cc53-cloudmnet.1
)

replace github.com/openGemini/openGemini/open_src/influx/influxql => ./protocol/influxql
