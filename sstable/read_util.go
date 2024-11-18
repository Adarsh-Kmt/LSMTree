package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/Adarsh-Kmt/LSMTree/proto_files"
	"google.golang.org/protobuf/proto"
)

func (sst *SSTable) ReadSSTTable() (kv []*proto_files.KeyValuePair, err error) {

	logger.Println("///////////////////////////////////////////////")
	logger.Println("/////////        READ PROCESS        /////////")
	logger.Println("///////////////////////////////////////////////")
	logger.Println()
	var f *os.File
	var headerBlock *SSTableHeader
	var indexBlock *proto_files.IndexBlock

	if f, err = os.Open(fmt.Sprintf("%s/%s", SST_Directory, sst.FileName)); err != nil {
		return nil, err
	}

	defer f.Close()

	if headerBlock, err = ReadHeaderBlock(f); err != nil {
		return nil, err
	}

	if indexBlock, err = ReadIndexBlock(f, headerBlock.IndexBlockOffset, headerBlock.MetaDataBlockOffset); err != nil {
		return nil, err
	}

	if _, err = ReadMetaDataBlock(f, headerBlock.MetaDataBlockOffset); err != nil {
		return nil, err
	}

	if kv, err = ReadAllDataPartitions(f, headerBlock, indexBlock); err != nil {
		return nil, err
	}
	logger.Println()
	logger.Println("///////////////////////////////////////////////")
	logger.Println("/////////        READ PROCESS        /////////")
	logger.Println("///////////////////////////////////////////////")
	logger.Println()
	return kv, nil
}

func ReadHeaderBlock(f *os.File) (*SSTableHeader, error) {

	f.Seek(0, 0)
	headerBytes := make([]byte, 24)

	var n int
	var err error
	logger.Println("------ HEADER BLOCK -------")
	logger.Println()
	if n, err = f.Read(headerBytes); err != nil {
		return nil, err
	} else {
		logger.Printf("read %d bytes of header data...", n)
	}

	var header SSTableHeader

	if err = binary.Read(bytes.NewBuffer(headerBytes), binary.LittleEndian, &header); err != nil {
		return nil, err
	}

	logger.Printf("header block => data block offset : %d, index block offset : %d meta data block offset : %d", header.DataBlockOffset, header.IndexBlockOffset, header.MetaDataBlockOffset)
	logger.Println()
	logger.Println("------ HEADER BLOCK -------")
	logger.Println()
	return &header, nil
}

func ReadIndexBlock(f *os.File, indexBlockOffset int64, metaDataBlockOffset int64) (*proto_files.IndexBlock, error) {

	f.Seek(indexBlockOffset, 0)
	logger.Println("------ INDEX BLOCK -------")
	logger.Println()
	logger.Printf("reading index block from file, starting at index %d, with size %d", indexBlockOffset, metaDataBlockOffset-indexBlockOffset)

	indexBlockBytes := make([]byte, metaDataBlockOffset-indexBlockOffset)

	f.Read(indexBlockBytes)

	var IndexBlock proto_files.IndexBlock

	if err := proto.Unmarshal(indexBlockBytes, &IndexBlock); err != nil {
		return nil, err
	}
	logger.Printf("length of index block %d", len(IndexBlock.Index))
	logger.Println()
	for _, dataBlockIndex := range IndexBlock.Index {
		logger.Printf("min key %d offset %d ", dataBlockIndex.MinKey, dataBlockIndex.Offset)
	}
	logger.Println()
	logger.Println("------ INDEX BLOCK -------")
	logger.Println()

	return &IndexBlock, nil
}

func ReadAllDataPartitions(f *os.File, headerBlock *SSTableHeader, indexBlock *proto_files.IndexBlock) (kv []*proto_files.KeyValuePair, err error) {

	kv = make([]*proto_files.KeyValuePair, 0)

	for i := 0; i < len(indexBlock.Index); i++ {

		var dataBlock *proto_files.DataBlock

		var startOffset int64
		var endOffset int64
		if i != len(indexBlock.Index)-1 {
			startOffset = indexBlock.Index[i].Offset + headerBlock.DataBlockOffset
			endOffset = indexBlock.Index[i+1].Offset + headerBlock.DataBlockOffset
		} else {
			startOffset = indexBlock.Index[i].Offset + headerBlock.DataBlockOffset
			endOffset = headerBlock.IndexBlockOffset
		}

		if dataBlock, err = ReadDataPartition(f, startOffset, endOffset); err != nil {
			return nil, err
		}

		kv = append(kv, dataBlock.Data...)

	}

	return kv, nil
}

func ReadDataPartition(f *os.File, startOffset int64, endOffset int64) (*proto_files.DataBlock, error) {

	logger.Println("------ DATA PARTITION -------")
	logger.Println()
	logger.Printf("starting to read data block from offset %d until offset %d", startOffset, endOffset)
	logger.Println()
	f.Seek(startOffset, 0)

	dataBlockBytes := make([]byte, endOffset-startOffset)

	f.Read(dataBlockBytes)

	var DataBlock proto_files.DataBlock

	if err := proto.Unmarshal(dataBlockBytes, &DataBlock); err != nil {
		return nil, err
	}

	for index := range DataBlock.Data {

		logger.Printf("key : %d value %s", DataBlock.Data[index].Key, DataBlock.Data[index].Value)
	}
	logger.Println()
	logger.Println("------ DATA PARTITION -------")
	logger.Println()
	return &DataBlock, nil

}
func ReadMetaDataBlock(f *os.File, metaDataBlockOffset int64) (*proto_files.MetaDataBlock, error) {

	f.Seek(metaDataBlockOffset, 0)

	logger.Println("------ META DATA BLOCK -------")
	logger.Println()
	logger.Printf("reading meta data block...")
	logger.Println()

	fileInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}
	metaDataBlockBytes := make([]byte, fileInfo.Size()-metaDataBlockOffset)

	f.Read(metaDataBlockBytes)

	var metaDataBlock proto_files.MetaDataBlock

	if err := proto.Unmarshal(metaDataBlockBytes, &metaDataBlock); err != nil {
		return nil, err
	}

	logger.Printf("min key : %d max key : %d ", metaDataBlock.MinKey, metaDataBlock.MaxKey)
	logger.Println()
	logger.Println("------ META DATA BLOCK -------")
	logger.Println()

	return &metaDataBlock, nil

}
