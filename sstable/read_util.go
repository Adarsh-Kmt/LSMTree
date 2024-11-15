package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/Adarsh-Kmt/LSMTree/proto_files"
	"google.golang.org/protobuf/proto"
)

func (sst *SSTable) ReadSSTTable() error {

	var err error
	var f *os.File
	var header *SSTableHeader

	f, err = os.Open(fmt.Sprintf("%s/%s", sst_directory, sst.fileName))

	if err != nil {
		return err
	}

	defer f.Close()

	header, err = ReadHeaderBlock(f)

	if err != nil {
		return err
	}

	_, err = ReadIndexBlock(f, header.IndexBlockOffset, header.MetaDataBlockOffset)
	if err != nil {
		return err
	}

	_, err = ReadMetaDataBlock(f, header.MetaDataBlockOffset)

	if err != nil {
		return err
	}

	_, err = ReadDataBlock(f, header.DataBlockOffset, header.MetaDataBlockOffset)

	if err != nil {
		return err
	}
	return nil
}

func ReadHeaderBlock(f *os.File) (*SSTableHeader, error) {

	f.Seek(0, 0)
	headerBytes := make([]byte, 24)

	var n int
	var err error

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

	return &header, nil
}

func ReadIndexBlock(f *os.File, indexBlockOffset int64, metaDataBlockOffset int64) (*proto_files.IndexBlock, error) {

	f.Seek(indexBlockOffset, 0)
	logger.Printf("reading index block from file, starting at index %d, with size %d", indexBlockOffset, metaDataBlockOffset-indexBlockOffset)
	indexBlockBytes := make([]byte, metaDataBlockOffset-indexBlockOffset)

	f.Read(indexBlockBytes)

	var IndexBlock proto_files.IndexBlock

	if err := proto.Unmarshal(indexBlockBytes, &IndexBlock); err != nil {
		return nil, err
	}
	logger.Printf("length of index block %d", len(IndexBlock.Index))
	for _, dataBlockIndex := range IndexBlock.Index {
		logger.Printf("min key %d offset %d ", dataBlockIndex.MinKey, dataBlockIndex.Offset)
	}

	return &IndexBlock, nil
}

func ReadDataBlock(f *os.File, startOffset int64, endOffset int64) (*proto_files.DataBlock, error) {

	logger.Printf("starting to read data block from offset %d until offset %d", startOffset, endOffset)
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

	return &DataBlock, nil

}

func ReadMetaDataBlock(f *os.File, metaDataBlockOffset int64) (*proto_files.MetaDataBlock, error) {

	f.Seek(metaDataBlockOffset, 0)

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
	logger.Printf("reading meta data block...")
	logger.Printf("min key : %d max key : %d ", metaDataBlock.MinKey, metaDataBlock.MaxKey)

	return &metaDataBlock, nil

}
