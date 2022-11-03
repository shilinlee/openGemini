package config

import (
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/openGemini/openGemini/lib/errno"
)

type conf struct {
	C Heimdall `toml:"heimdall"`
}

func newConf() *conf {
	c := NewHeimdall()
	return &conf{c}
}

func Test_CorrectConfig(t *testing.T) {
	confStr := `
	[heimdall]
		enabled = true
		pyworker-addr = ["127.0.0.1:6666"]
		connect-pool-size = 1  # default: 30, connection pool to pyworker
		result-wait-timeout = 10  # default: 30 second
  	[heimdall.detect]
		algorithm = ['BatchDIFFERENTIATEAD']
		config_filename = ['detect_base']
 	[heimdall.fit_detect]
		algorithm = ['DIFFERENTIATEAD']
		config_filename = ['detect_base']
  	[heimdall.predict]
		algorithm = ['METROPD']
		config_filename = ['predict_base']
  	[heimdall.fit]
		algorithm = ['METROPD']
		config_filename = ['fit_base']
	`
	c := newConf()
	toml.Decode(confStr, c)
	if err := c.C.Validate(); err != nil {
		t.Fatal(err)
	}
}

func Test_InvalidPoolSize(t *testing.T) {
	confStr := `
	[heimdall]
		enabled = true
		connect-pool-size = 0  # default: 30, connection pool to pyworker
	`
	c := newConf()
	toml.Decode(confStr, c)
	if err := c.C.Validate(); !errno.Equal(err, errno.InvalidPoolSize) {
		t.Fatal(err)
	}
}

func Test_InvalidResultWaitTimeout(t *testing.T) {
	confStr := `
	[heimdall]
		enabled = true
		connect-pool-size = 1  # default: 30, connection pool to pyworker
		result-wait-timeout = 0  # default: 30 second
	`
	c := newConf()
	toml.Decode(confStr, c)
	if err := c.C.Validate(); !errno.Equal(err, errno.InvalidResultWaitTimeout) {
		t.Fatal(err)
	}
}

func Test_InvalidAddr(t *testing.T) {
	confStr := `
	[heimdall]
		enabled = true
		connect-pool-size = 1  # default: 30, connection pool to pyworker
		result-wait-timeout = 1  # default: 30 second
		pyworker-addr = ["abc:6666"]
	`
	c := newConf()
	toml.Decode(confStr, c)
	if err := c.C.Validate(); !errno.Equal(err, errno.InvalidAddr) {
		t.Fatal(err)
	}
}

func Test_InvalidAddr2(t *testing.T) {
	confStr := `
	[heimdall]
		enabled = true
		connect-pool-size = 1  # default: 30, connection pool to pyworker
		result-wait-timeout = 1  # default: 30 second
		pyworker-addr = ["abc"]
	`
	c := newConf()
	toml.Decode(confStr, c)
	if err := c.C.Validate(); !errno.Equal(err, errno.InvalidAddr) {
		t.Fatal(err)
	}
}

func Test_InvalidAddr3(t *testing.T) {
	confStr := `
	[heimdall]
		enabled = true
		connect-pool-size = 1  # default: 30, connection pool to pyworker
		result-wait-timeout = 1  # default: 30 second
	`
	c := newConf()
	toml.Decode(confStr, c)
	if err := c.C.Validate(); !errno.Equal(err, errno.InvalidAddr) {
		t.Fatal(err)
	}
}

func Test_InvalidPort(t *testing.T) {
	confStr := `
	[heimdall]
		enabled = true
		connect-pool-size = 1  # default: 30, connection pool to pyworker
		result-wait-timeout = 1  # default: 30 second
		pyworker-addr = ["127.0.0.1:abc"]
	`
	c := newConf()
	toml.Decode(confStr, c)
	if err := c.C.Validate(); !errno.Equal(err, errno.InvalidPort) {
		t.Fatal(err)
	}
}

func Test_InvalidPort2(t *testing.T) {
	confStr := `
	[heimdall]
		enabled = true
		connect-pool-size = 1  # default: 30, connection pool to pyworker
		result-wait-timeout = 1  # default: 30 second
		pyworker-addr = ["127.0.0.1:-1"]
	`
	c := newConf()
	toml.Decode(confStr, c)
	if err := c.C.Validate(); !errno.Equal(err, errno.InvalidPort) {
		t.Fatal(err)
	}
}

func Test_IncompleteAlgoConf(t *testing.T) {
	confStr := `
	[heimdall]
		enabled = true
		connect-pool-size = 1  # default: 30, connection pool to pyworker
		result-wait-timeout = 1  # default: 30 second
		pyworker-addr = ["127.0.0.1:6666"]
	[heimdall.detect]
		algorithm = ['BatchDIFFERENTIATEAD']
		config_filename = []
	`
	c := newConf()
	toml.Decode(confStr, c)
	if err := c.C.Validate(); !errno.Equal(err, errno.AlgoConfNotFound) {
		t.Fatal(err)
	}
}

func Test_IncompleteAlgo(t *testing.T) {
	confStr := `
	[heimdall]
		enabled = true
		connect-pool-size = 1  # default: 30, connection pool to pyworker
		result-wait-timeout = 1  # default: 30 second
		pyworker-addr = ["127.0.0.1:6666"]
	[heimdall.detect]
		algorithm = []
		config_filename = ["detect"]
	`
	c := newConf()
	toml.Decode(confStr, c)
	if err := c.C.Validate(); !errno.Equal(err, errno.AlgoNotFound) {
		t.Fatal(err)
	}
}

func Test_CheckAlgoAndConfExistence(t *testing.T) {
	confStr := `
	[heimdall]
	enabled = true
	pyworker-addr = ["127.0.0.1:6666"]
	connect-pool-size = 1  # default: 30, connection pool to each pyworker
	result-wait-timeout = 10  # default: 30 second
	[heimdall.detect]
		algorithm = ['BatchDIFFERENTIATEAD']
		config_filename = ['detect_base']
	[heimdall.fit_detect]
		algorithm = ['DIFFERENTIATEAD']
		config_filename = ['detect_base']
	[heimdall.predict]
		algorithm = ['METROPD']
		config_filename = ['predict_base']
	[heimdall.fit]
		algorithm = ['METROPD']
		config_filename = ['fit_base']
	`
	c := newConf()
	toml.Decode(confStr, c)
	if err := c.C.Validate(); err != nil {
		t.Fatal(err)
	}

	if err := c.C.CheckAlgoAndConfExistence("BatchDIFFERENTIATEAD", "detect_base", "detect"); err != nil {
		t.Fatal(err)
	}
	if err := c.C.CheckAlgoAndConfExistence("BatchDIFFERENTIATEAD", "detect_base", "fit_detect"); !errno.Equal(err, errno.AlgoNotFound) {
		t.Fatal(err)
	}
	if err := c.C.CheckAlgoAndConfExistence("BatchDIFFERENTIATEAD", "fit_base", "detect"); !errno.Equal(err, errno.AlgoConfNotFound) {
		t.Fatal(err)
	}
	if err := c.C.CheckAlgoAndConfExistence("BatchDIFFERENTIATEAD", "detect_base", "abc"); !errno.Equal(err, errno.AlgoTypeNotFound) {
		t.Fatal(err)
	}
}
