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
package heimdall

import (
	"net"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/openGemini/openGemini/lib/errno"
)

// check if key is internal
func IsInternalKey(key string) bool {
	_, exist := internalKeySet[key]
	return exist
}

// return value from record's metadata according to key
func GetMetaValueFromRecord(data array.Record, key string) (string, *errno.Error) {
	md := data.Schema().Metadata()
	idx := md.FindKey(key)
	if idx == -1 {
		return "", errno.NewError(errno.MessageNotFound, key)
	}
	return md.Values()[idx], nil
}

func getConn(addr string) (net.Conn, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
