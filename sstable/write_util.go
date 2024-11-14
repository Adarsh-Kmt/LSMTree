package sstable

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"

	memtable "github.com/Adarsh-Kmt/LSMTree/memtable"
	"github.com/Adarsh-Kmt/LSMTree/proto_files"
	"github.com/willf/bloom"
	"google.golang.org/protobuf/proto"
)

const (
	sst_directory = "sst_files"
)

var (
	logger     = log.New(os.Stdout, "LSMTREE >> ", 0)
	sst_number = 1
)

func SSTableInit(memTable memtable.MEMTable) (*SSTable, error) {

	var err error
	var dataBlockBytes []byte
	var metaDataBlockBytes []byte
	var indexBlockBytes []byte
	sst := &SSTable{
		fileName:    fmt.Sprintf("sst_%d.dat", sst_number),
		bloomFilter: bloom.New(10, 10),
	}

	f, err := os.Create(fmt.Sprintf("%s/%s", sst_directory, sst.fileName))

	if err != nil {
		return nil, err
	}

	sst_number++
	defer f.Close()

	if dataBlockBytes, indexBlockBytes, err = CreateDataBlockAndIndexBlockBytes(memTable.GetAllItems()); err != nil {
		return nil, err
	}

	if metaDataBlockBytes, err = CreateMetaDataBytes(memTable.GetMinKey(), memTable.GetMaxKey()); err != nil {
		return nil, err
	}

	if err = sst.WriteHeaderBlock(f, len(dataBlockBytes), len(indexBlockBytes)); err != nil {
		return nil, err
	}
	logger.Printf("wrote %d bytes to header block...", 24)

	if err = binary.Write(f, binary.LittleEndian, dataBlockBytes); err != nil {
		return nil, err
	}
	logger.Printf("wrote %d bytes to data block...", len(dataBlockBytes))

	if err = binary.Write(f, binary.LittleEndian, indexBlockBytes); err != nil {
		return nil, err
	}

	logger.Printf("wrote %d bytes to index block...", len(indexBlockBytes))

	if err = binary.Write(f, binary.LittleEndian, metaDataBlockBytes); err != nil {
		return nil, err
	}
	logger.Printf("wrote %d bytes to meta data block...", len(metaDataBlockBytes))

	return sst, nil
}

func (sst *SSTable) WriteHeaderBlock(f *os.File, dataBlockSize int, indexBlockSize int) error {

	header := &SSTableHeader{
		DataBlockOffset:     int64(24),
		IndexBlockOffset:    int64(24 + dataBlockSize),
		MetaDataBlockOffset: int64(24 + dataBlockSize + indexBlockSize),
	}

	err := binary.Write(f, binary.LittleEndian, header)

	return err
}

func CreateDataBlockAndIndexBlockBytes(keys []int, values []string) (dataBlockBytes []byte, indexBlockBytes []byte, err error) {

	dataBlockBytes = make([]byte, 0)

	indexBlock := &proto_files.IndexBlock{}
	Index := make([]*proto_files.IndexEntry, 0)
	dataBlockOffset := 0

	for i := 0; i < len(keys); i += 5 {

		dbIndex := proto_files.IndexEntry{Offset: int64(dataBlockOffset)}

		kv := make([]*proto_files.KeyValuePair, 0)
		for j := i; j < i+5 && j < len(keys); j++ {

			if j == i {
				dbIndex.MinKey = int64(keys[j])
			}
			kv = append(kv, &proto_files.KeyValuePair{Key: int64(keys[j]), Value: values[j]})
		}

		if dbBytes, err := proto.Marshal(&proto_files.DataBlock{Data: kv}); err != nil {
			return nil, nil, err

		} else {
			dataBlockOffset += len(dbBytes)
			dataBlockBytes = append(dataBlockBytes, dbBytes...)
			logger.Printf("data block %v with size %d", kv, len(dbBytes))
		}

		// if dbIndexBytes, err := proto.Marshal(&dbIndex); err != nil {
		// 	return nil, nil, err
		// } else {
		// 	indexBlockBytes = append(indexBlockBytes, dbIndexBytes...)
		// 	logger.Printf("index block min key : %v offset : %d of size %d", dbIndex.MinKey, dbIndex.Offset, len(dbIndexBytes))
		// }
		logger.Printf("index block min key : %v offset : %d", dbIndex.MinKey, dbIndex.Offset)

		Index = append(Index, &dbIndex)

	}

	indexBlock.Index = Index
	if indexBlockBytes, err := proto.Marshal(indexBlock); err != nil {
		return nil, nil, err
	} else {
		return dataBlockBytes, indexBlockBytes, nil
	}

}
func CreateDataBlockBytes(memTable memtable.MEMTable) ([]byte, error) {

	var dataBlockBytes []byte

	var err error

	keys, values := memTable.GetAllItems()

	kv := make([]*proto_files.KeyValuePair, 0)

	for index := range keys {
		kv = append(kv, &proto_files.KeyValuePair{Key: int64(keys[index]), Value: values[index]})
	}
	dataBlock := &proto_files.DataBlock{
		Data: kv,
	}

	dataBlockBytes, err = proto.Marshal(dataBlock)

	if err != nil {
		return nil, err
	}

	return dataBlockBytes, nil

}

func CreateMetaDataBytes(minKey int, maxKey int) ([]byte, error) {

	var metaDataBytes []byte
	var err error
	md := &proto_files.MetaDataBlock{
		MinKey: int64(minKey),
		MaxKey: int64(maxKey),
	}

	if metaDataBytes, err = proto.Marshal(md); err != nil {
		return nil, err
	}

	return metaDataBytes, nil

}
