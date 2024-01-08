/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
package immutable

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sort"

	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/request"
	"github.com/openGemini/openGemini/lib/util"
)

const (
	MetaIndexLimitNum         = 16
	MetaIndexHeaderSize int64 = 16
	MetaIndexItemSize         = int64(util.Int64SizeBytes*3 + util.Uint64SizeBytes + util.Uint32SizeBytes)
)

type DetachedMetaIndexReader struct {
	r fileops.BasicFileReader
}

func NewDetachedMetaIndexReader(path string, obsOpts *obs.ObsOptions) (*DetachedMetaIndexReader, error) {
	fd, err := obs.OpenObsFile(path, MetaIndexFile, obsOpts)
	if err != nil {
		return nil, err
	}
	dr := fileops.NewFileReader(fd, nil)
	return &DetachedMetaIndexReader{r: dr}, nil
}

func (reader *DetachedMetaIndexReader) ReadMetaIndex(offset, length []int64) ([]*MetaIndex, error) {
	c := make(chan *request.StreamReader, 1)
	reader.r.StreamReadBatch(offset, length, c, MetaIndexLimitNum)
	metaIndexs := make([]*MetaIndex, len(offset))
	i := 0
	for r := range c {
		if r.Err != nil {
			return nil, r.Err
		}
		metaIndexs[i] = &MetaIndex{}
		if len(r.Content) < crcSize {
			return nil, fmt.Errorf("get wrong data")
		}
		if binary.BigEndian.Uint32(r.Content[:crcSize]) != crc32.ChecksumIEEE(r.Content[crcSize:]) {
			return nil, fmt.Errorf("get wrong data")
		}
		r.Content = r.Content[crcSize:]
		_, err := metaIndexs[i].unmarshalDetached(r.Content)
		if err != nil {
			return nil, err
		}
		i += 1
	}
	sort.Slice(metaIndexs, func(i, j int) bool {
		return metaIndexs[i].offset < metaIndexs[j].offset
	})
	return metaIndexs, nil
}

func GetMetaIndexOffsetAndLengthByChunkId(chunkId int64) (offset, length int64) {
	return chunkId*(crcSize+MetaIndexItemSize) + MetaIndexHeaderSize, crcSize + MetaIndexItemSize
}

func GetMetaIndexChunkCount(obsOptions *obs.ObsOptions, dataPath string) (int64, error) {
	fd, err := OpenObsFile(dataPath, MetaIndexFile, obsOptions)
	if err != nil {
		return 0, err
	}
	miFileInfo, err := fd.Stat()
	if err != nil {
		return 0, err
	}
	miFileSize := miFileInfo.Size()
	return (miFileSize - MetaIndexHeaderSize) / (crcSize + MetaIndexItemSize), nil
}
