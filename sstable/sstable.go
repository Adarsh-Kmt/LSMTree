package sstable

import (
	"fmt"
	"os"
	"sync"

	"github.com/Adarsh-Kmt/LSMTree/proto_files"
	"github.com/willf/bloom"
)

type SSTableLevel struct {
	RWMutex             *sync.RWMutex
	LevelNumber         int
	NumberOfSSTables    int
	SSTables            []SSTable
	MaxNumberOfSSTables int
}

type SSTable struct {
	FileName    string
	BloomFilter *bloom.BloomFilter
}

type SSTableHeader struct {
	DataBlockOffset     int64
	IndexBlockOffset    int64
	MetaDataBlockOffset int64
}

func (sst *SSTable) Get(key int64) (value string, err error) {

	var f *os.File
	var headerBlock *SSTableHeader
	var metaDataBlock *proto_files.MetaDataBlock
	var indexBlock *proto_files.IndexBlock

	var startOffset int64
	var endOffset int64

	var index int

	if f, err = os.Open(fmt.Sprintf("%s/%s", SST_Directory, sst.FileName)); err != nil {
		return "", fmt.Errorf("error while opening sst file")
	}

	defer f.Close()

	if headerBlock, err = ReadHeaderBlock(f); err != nil {
		return "", fmt.Errorf("error while reading header block of sst file : err => %s", err.Error())
	}

	if metaDataBlock, err = ReadMetaDataBlock(f, headerBlock.MetaDataBlockOffset); err != nil {
		return "", fmt.Errorf("error while reading meta data block of sst file : err => %s", err.Error())
	}

	if metaDataBlock.MaxKey < key || metaDataBlock.MinKey > key {
		logger.Printf("based on meta data block of sst file, key is greater than max key / key is smaller than min key")
		return "", fmt.Errorf("key does not exist in key range, acording to meta data block")
	}
	if indexBlock, err = ReadIndexBlock(f, headerBlock.IndexBlockOffset, headerBlock.MetaDataBlockOffset); err != nil {
		return "", fmt.Errorf("error while reading index block of sst file : err => %s", err.Error())
	}

	index = searchInIndexBlock(key, indexBlock)

	logger.Printf("search in %dth block in data block : ", index)
	startOffset = headerBlock.DataBlockOffset + indexBlock.Index[index].Offset

	if index == len(indexBlock.Index)-1 {
		endOffset = headerBlock.IndexBlockOffset
	} else {
		endOffset = headerBlock.DataBlockOffset + indexBlock.Index[index+1].Offset
	}

	dataBlock, err := ReadDataPartition(f, startOffset, endOffset)

	//logger.Printf("length of data partition : %d", len(dataBlock.Data))
	if err != nil {
		return "", fmt.Errorf("error while reading data block : err => %s", err.Error())
	}

	var found bool

	if value, found = searchInDataBlock(key, dataBlock); found {
		return value, nil
	}

	return "", fmt.Errorf("key value pair not found in sst table")
}
