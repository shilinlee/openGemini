package heimdall

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func Test_ClientWriteRead(t *testing.T) {
	addr := "127.0.0.1:6666"
	if err := MockPyWorker(addr); err != nil {
		t.Fatal(err)
	}

	observedZapCore, observedLog := observer.New(zap.DebugLevel)
	observedLogger := zap.New(observedZapCore)
	l := logger.NewLogger(errno.ModuleHeimdall)
	l.SetZapLogger(observedLogger)

	respChan := make(chan array.Record)
	cnt := new(int32)
	cli, err := newClient(addr, l, respChan, cnt)
	if err != nil {
		t.Fatal(err)
	}

	data := BuildNumericRecord()
	if err := cli.Write(data); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)
	timer := time.After(time.Second)
	isResponseRead := false
WAITRESP:
	for {
		select {
		case <-timer:
			break WAITRESP
		case _, ok := <-respChan:
			if !ok {
				t.Fatal("response close")
			}
			isResponseRead = true
			break WAITRESP
		}
	}
	if !isResponseRead {
		t.Fatal("client not read response")
	}

	cli.Close()
	for _, log := range observedLog.All() {
		if log.Level > zap.InfoLevel {
			t.Fatal("client close with error")
		}
	}

	if atomic.LoadInt32(cnt) != 0 {
		t.Fatal("client release but reference count not reduce")
	}
}
