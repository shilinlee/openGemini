package config

import (
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
)

const (
	DefaultPoolSize    int = 30
	DefaultWaitTimeout int = 30
)

type algorithmType string

const (
	Fit       algorithmType = "fit"
	Predict   algorithmType = "predict"
	Detect    algorithmType = "detect"
	FitDetect algorithmType = "fit_detect"
)

type Heimdall struct {
	Enabled           bool       `toml:"enabled"`
	PyWorkerAddr      []string   `toml:"pyworker-addr"`
	ConnPoolSize      int        `toml:"connect-pool-size"`
	ResultWaitTimeout int        `toml:"result-wait-timeout"`
	FitDetect         algoConfig `toml:"fit_detect"`
	Detect            algoConfig `toml:"detect"`
	Predict           algoConfig `toml:"predict"`
	Fit               algoConfig `toml:"fit"`
}

type algoConfig struct {
	Algorithm  []string `toml:"algorithm"`
	ConfigFile []string `toml:"config_filename"`
}

func NewHeimdall() Heimdall {
	return Heimdall{
		ConnPoolSize:      DefaultPoolSize,
		ResultWaitTimeout: DefaultWaitTimeout,
	}
}

func (c *Heimdall) ApplyEnvOverrides(_ func(string) string) error {
	return nil
}

func (c *Heimdall) Validate() *errno.Error {
	if !c.Enabled {
		return nil
	}

	if c.ConnPoolSize <= 0 {
		return errno.NewError(errno.InvalidPoolSize)
	}

	if c.ResultWaitTimeout <= 0 {
		return errno.NewError(errno.InvalidResultWaitTimeout)
	}

	if err := c.checkUrl(); err != nil {
		return err
	}

	if err := c.Predict.validateAlgoAndConf(); err != nil {
		return err
	}
	if err := c.Detect.validateAlgoAndConf(); err != nil {
		return err
	}
	if err := c.FitDetect.validateAlgoAndConf(); err != nil {
		return err
	}
	if err := c.Fit.validateAlgoAndConf(); err != nil {
		return err
	}

	return nil
}

func (c *Heimdall) checkUrl() *errno.Error {
	if len(c.PyWorkerAddr) == 0 {
		return errno.NewError(errno.InvalidAddr)
	}
	for _, addr := range c.PyWorkerAddr {
		sp := strings.Split(addr, ":")
		if len(sp) != 2 {
			return errno.NewError(errno.InvalidAddr)
		}
		ip, port := sp[0], sp[1]
		if net.ParseIP(ip) == nil {
			return errno.NewError(errno.InvalidAddr)
		}
		i, err := strconv.Atoi(port)
		if err != nil {
			return errno.NewError(errno.InvalidPort)
		}
		if i <= 0 {
			return errno.NewError(errno.InvalidPort)
		}
	}
	return nil
}

func (c *Heimdall) GetWaitTimeout() time.Duration {
	return time.Duration(c.ResultWaitTimeout * int(time.Second))
}

func (c *Heimdall) CheckAlgoAndConfExistence(algo, conf, algorithmType string) *errno.Error {
	switch algorithmType {
	case string(Fit):
		return c.Fit.checkAlgoAndConfigExistence(algo, conf)
	case string(FitDetect):
		return c.FitDetect.checkAlgoAndConfigExistence(algo, conf)
	case string(Detect):
		return c.Detect.checkAlgoAndConfigExistence(algo, conf)
	case string(Predict):
		return c.Predict.checkAlgoAndConfigExistence(algo, conf)
	default:
		return errno.NewError(errno.AlgoTypeNotFound)
	}
}

func checkExistence(elem string, arr []string) bool {
	for _, item := range arr {
		if item == elem {
			return true
		}
	}
	return false
}

func (a *algoConfig) GetAlgorithms() []string {
	return a.Algorithm
}

func (a *algoConfig) GetAlgoConfigFiles() []string {
	return a.ConfigFile
}

func (a *algoConfig) checkAlgoAndConfigExistence(algo, conf string) *errno.Error {
	if !checkExistence(algo, a.Algorithm) {
		return errno.NewError(errno.AlgoNotFound)
	}
	if !checkExistence(conf, a.ConfigFile) {
		return errno.NewError(errno.AlgoConfNotFound)
	}
	return nil
}

func (a *algoConfig) validateAlgoAndConf() *errno.Error {
	if len(a.ConfigFile) == 0 && len(a.Algorithm) != 0 {
		return errno.NewError(errno.AlgoConfNotFound)
	}
	if len(a.Algorithm) == 0 && len(a.ConfigFile) != 0 {
		return errno.NewError(errno.AlgoNotFound)
	}
	return nil
}
