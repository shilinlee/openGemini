//go:build scc && !windows
// +build scc,!windows

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

package crypto

import (
	cryptoapi "codehub-g.huawei.com/sec/seccomponent"
	"go.uber.org/zap"
)

func init() {
	SetDecipher(&SccDecipher{})
}

type SccDecipher struct {
}

func (d *SccDecipher) Initialize(conf string) {
	if conf == "" {
		return
	}

	logger.Info("begin init SecComponent", zap.String("config", conf))
	err := cryptoapi.Initialize(conf)
	if err != nil {
		logger.Error("failed to init SecComponent", zap.Error(err))
		return
	}
	logger.Info("success to init SecComponent")
	SetDecipher(&SccDecipher{})
}

func (d *SccDecipher) Decrypt(s string) (string, error) {
	return cryptoapi.Decrypt(s)
}

func (d *SccDecipher) Destruct() {
	err := cryptoapi.Finalize()
	if err != nil {
		logger.Info("scc finalize failed", zap.Error(err))
		return
	}
}
