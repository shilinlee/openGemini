package heimdall

import (
	"fmt"

	"github.com/apache/arrow/go/arrow/array"
)

// check if key is internal
func IsInternalKey(key string) bool {
	_, exist := internalKeySet[key]
	return exist
}

// return value from record's metadata according to key
func GetMetaValueFromRecord(data array.Record, key string) (string, error) {
	md := data.Schema().Metadata()
	idx := md.FindKey(key)
	if idx == -1 {
		return "", fmt.Errorf("%s not found", key)
	}
	return md.Values()[idx], nil
}
