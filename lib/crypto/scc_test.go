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

package crypto_test

import (
	"os"
	"testing"

	cryptoapi "codehub-g.huawei.com/sec/seccomponent"
	"github.com/openGemini/openGemini/lib/crypto"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/stretchr/testify/require"
)

func initSCCConf(t *testing.T) string {
	crypto.SetLogger(logger.GetLogger())
	dir := t.TempDir() + "/seccomponent"
	conf := dir + "/conf/scc.conf"
	require.NoError(t, os.MkdirAll(dir+"/conf", 0700))
	require.NoError(t, os.MkdirAll(dir+"/ks/backup", 0700))
	require.NoError(t, os.MkdirAll(dir+"/log", 0700))

	fp, err := os.OpenFile(conf, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	require.NoError(t, err)
	_, err = fp.WriteString(`[CRYPTO]
primaryKeyStoreFile =` + dir + `/ks/primary.ks
standbyKeyStoreFile =` + dir + `/ks/standby.ks
backupFolderName=` + dir + `/ks/backup/
domainCount=8
logFilePath=` + dir + `/log/
logFileName=scc`)
	require.NoError(t, err)
	require.NoError(t, fp.Close())

	return conf
}

func TestSCCDecrypt(t *testing.T) {
	txt := "abcd1234"
	crypto.SetDecipher(nil)
	require.Equal(t, txt, crypto.Decrypt(txt))

	crypto.Initialize(initSCCConf(t))
	defer crypto.Destruct()

	encrypt, err := cryptoapi.Encrypt(txt)
	require.NoError(t, err)

	require.Equal(t, txt, crypto.Decrypt(encrypt))

	encFile := t.TempDir() + "/enc.data"
	require.NoError(t, os.WriteFile(encFile, []byte(encrypt), 0600))

	require.Equal(t, txt, crypto.DecryptFromFile(encFile))

	// invalid input
	require.Equal(t, "", crypto.Decrypt(txt))
}
