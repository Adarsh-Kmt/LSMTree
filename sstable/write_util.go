package sstable

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"

	"github.com/Adarsh-Kmt/LSMTree/proto_files"
	"github.com/willf/bloom"
	"google.golang.org/protobuf/proto"
)

const (
	SST_Directory = "sst_files"
)

var (
	//logFile, _ = os.OpenFile("log_file.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	logger     = log.New(os.Stdout, "LSMTREE >> ", 0)
	SST_Number = 1
)

func SSTableInit(kv []*proto_files.KeyValuePair, minKey int64, maxKey int64) (*SSTable, error) {
	logger.Println("///////////////////////////////////////////////")
	logger.Println("////////        WRITE PROCESS        /////////")
	logger.Println("///////////////////////////////////////////////")
	logger.Println()
	var err error
	var dataBlockBytes []byte
	var metaDataBlockBytes []byte
	var indexBlockBytes []byte
	sst := &SSTable{
		FileName:    fmt.Sprintf("sst_%d.dat", SST_Number),
		BloomFilter: bloom.New(10, 10),
	}

	f, err := os.Create(fmt.Sprintf("%s/%s", SST_Directory, sst.FileName))

	if err != nil {
		return nil, err
	}

	SST_Number++
	defer f.Close()

	if dataBlockBytes, indexBlockBytes, err = CreateDataBlockAndIndexBlockBytes(kv); err != nil {
		return nil, err
	}

	if metaDataBlockBytes, err = CreateMetaDataBytes(minKey, maxKey); err != nil {
		return nil, err
	}

	if err = WriteHeaderBlock(f, len(dataBlockBytes), len(indexBlockBytes)); err != nil {
		return nil, err
	}

	if err = binary.Write(f, binary.LittleEndian, dataBlockBytes); err != nil {
		return nil, err
	}
	//logger.Printf("wrote %d bytes to data block...", len(dataBlockBytes))

	if err = binary.Write(f, binary.LittleEndian, indexBlockBytes); err != nil {
		return nil, err
	}

	//logger.Printf("wrote %d bytes to index block...", len(indexBlockBytes))

	if err = binary.Write(f, binary.LittleEndian, metaDataBlockBytes); err != nil {
		return nil, err
	}
	//logger.Printf("wrote %d bytes to meta data block...", len(metaDataBlockBytes))

	logger.Println()
	logger.Println("///////////////////////////////////////////////")
	logger.Println("////////        WRITE PROCESS        /////////")
	logger.Println("///////////////////////////////////////////////")
	return sst, nil
}

func WriteHeaderBlock(f *os.File, dataBlockSize int, indexBlockSize int) error {

	logger.Println("------ HEADER BLOCK -------")
	logger.Println()
	logger.Printf("wrote %d bytes to header block...", 24)
	logger.Println()
	header := &SSTableHeader{
		DataBlockOffset:     int64(24),
		IndexBlockOffset:    int64(24 + dataBlockSize),
		MetaDataBlockOffset: int64(24 + dataBlockSize + indexBlockSize),
	}

	err := binary.Write(f, binary.LittleEndian, header)
	logger.Printf("header block => %v", header)

	logger.Println()
	logger.Println("------ HEADER BLOCK -------")
	logger.Println()
	return err
}

func CreateDataBlockAndIndexBlockBytes(kv []*proto_files.KeyValuePair) (dataBlockBytes []byte, indexBlockBytes []byte, err error) {

	dataBlockBytes = make([]byte, 0)

	indexBlock := &proto_files.IndexBlock{}
	Index := make([]*proto_files.IndexEntry, 0)
	dataBlockOffset := 0

	for i := 0; i < len(kv); i += 5 {

		logger.Println("------ DATA PARTITION -------")
		logger.Println()
		dbIndex := proto_files.IndexEntry{Offset: int64(dataBlockOffset)}

		currkv := make([]*proto_files.KeyValuePair, 0)
		for j := i; j < i+5 && j < len(kv); j++ {

			if j == i {
				dbIndex.MinKey = int64(kv[j].Key)
			}
			currkv = append(currkv, kv[j])
		}
		logger.Printf("data partition => %v", currkv)
		logger.Println()
		logger.Println("------ DATA PARTITION -------")
		logger.Println()
		if dbBytes, err := proto.Marshal(&proto_files.DataBlock{Data: currkv}); err != nil {
			return nil, nil, err

		} else {
			dataBlockOffset += len(dbBytes)
			dataBlockBytes = append(dataBlockBytes, dbBytes...)
			logger.Printf("data block %v with size %d", currkv, len(dbBytes))
		}

		Index = append(Index, &dbIndex)

	}

	logger.Println("------ INDEX BLOCK -------")
	logger.Println()
	for _, indexEntry := range Index {
		logger.Printf("index block min key : %v offset : %d", indexEntry.MinKey, indexEntry.Offset)

	}
	logger.Println()
	logger.Println("------ INDEX BLOCK -------")
	logger.Println()
	indexBlock.Index = Index
	if indexBlockBytes, err := proto.Marshal(indexBlock); err != nil {
		return nil, nil, err
	} else {
		return dataBlockBytes, indexBlockBytes, nil
	}

}

func CreateMetaDataBytes(minKey int64, maxKey int64) ([]byte, error) {

	logger.Println("------ META DATA BLOCK -------")
	logger.Println()
	var metaDataBytes []byte
	var err error
	md := &proto_files.MetaDataBlock{
		MinKey: minKey,
		MaxKey: maxKey,
	}

	if metaDataBytes, err = proto.Marshal(md); err != nil {
		return nil, err
	}
	logger.Printf("meta data block => %v", md)
	logger.Println()
	logger.Println("------ META DATA BLOCK -------")
	logger.Println()
	return metaDataBytes, nil

}
