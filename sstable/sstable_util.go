package sstable

import "github.com/Adarsh-Kmt/LSMTree/proto_files"

//searches for the appropriate index entry
func searchInIndexBlock(key int64, indexBlock *proto_files.IndexBlock) (indexEntryPosition int) {

	l := 0
	h := len(indexBlock.Index) - 1

	for l <= h {

		mid := l + (h-l)/2

		if indexBlock.Index[mid].MinKey > key {
			h = mid - 1
		} else if indexBlock.Index[mid].MinKey <= key {

			indexEntryPosition = mid
			l = mid + 1
		}
	}

	return indexEntryPosition
}

func searchInDataBlock(key int64, dataBlock *proto_files.DataBlock) (value string, found bool) {

	l := 0
	h := len(dataBlock.Data) - 1

	for l <= h {

		mid := l + (h-l)/2

		midKey := dataBlock.Data[mid].Key
		logger.Printf("current key value pair : key %d value %s", midKey, dataBlock.Data[mid].Value)

		if midKey == key {
			return dataBlock.Data[mid].Value, true
		} else if midKey < key {
			l = mid + 1
		} else {
			h = mid - 1
		}
	}
	return "", false
}
func MergeDataBlock(kv1 []proto_files.KeyValuePair, sequenceNumber1 int64, kv2 []proto_files.KeyValuePair, sequenceNumber2 int64) (result []proto_files.KeyValuePair) {

	if kv2 == nil {
		return kv1
	}

	if kv1 == nil {
		return kv2
	}

	result = make([]proto_files.KeyValuePair, 0)

	x := 0
	y := 0

	for x < len(kv1) && y < len(kv2) {

		if kv1[x].Value == "tombstone" {
			x++
			continue
		}
		if kv2[y].Value == "tombstone" {
			y++
			continue
		}
		if kv1[x].Key < kv2[y].Key {
			if kv1[x].Value != "tombstone" {
				result = append(result, proto_files.KeyValuePair{Key: kv1[x].Key, Value: kv1[x].Value})
			}
			x++
		} else if kv1[x].Key > kv2[y].Key {
			if kv2[y].Value != "tombstone" {
				result = append(result, proto_files.KeyValuePair{Key: kv2[y].Key, Value: kv2[y].Value})
			}
			y++

		} else {
			if sequenceNumber1 > sequenceNumber2 {
				if kv1[x].Value != "tombstone" {
					result = append(result, proto_files.KeyValuePair{Key: kv1[x].Key, Value: kv1[x].Value})
				}
				x++
			} else {
				if kv2[y].Value != "tombstone" {
					result = append(result, proto_files.KeyValuePair{Key: kv2[y].Key, Value: kv2[y].Value})
				}
				y++
			}
		}
	}

	return result

}
