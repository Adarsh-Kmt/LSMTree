package lsmtree

import (
	"fmt"
	"math"
	"os"

	"github.com/Adarsh-Kmt/LSMTree/proto_files"
	sst "github.com/Adarsh-Kmt/LSMTree/sstable"
)

func (lsmtree *LSMTree) BackgroundCompaction() {

	defer lsmtree.Wg.Done()
	for {

		select {

		case <-lsmtree.ctx.Done():

			if compactionTriggered := lsmtree.TriggerCompaction(); !compactionTriggered {
				close(lsmtree.compactLevelChannel)

				return
			}
		case index := <-lsmtree.compactLevelChannel:

			if err := lsmtree.SizeTieredCompaction(index); err != nil {

				logger.Printf("error during compaction : %s", err.Error())
				return
			}
		}
	}
}

func (lsmtree *LSMTree) TriggerCompaction() (compactionTriggered bool) {

	compactionTriggered = false

	for index, level := range lsmtree.SSTableLevels {

		if index == len(lsmtree.SSTableLevels)-1 {
			continue
		}
		if level.NumberOfSSTables > level.MaxNumberOfSSTables {
			compactionTriggered = true
			lsmtree.SizeTieredCompaction(index)
		}
	}

	return compactionTriggered
}

func (lsmtree *LSMTree) SizeTieredCompaction(levelNumber int) (err error) {

	logger.Println("///////////////////////////////////////////////")
	logger.Println("/////////    COMPACTION PROCESS     ///////////")
	logger.Println("///////////////////////////////////////////////")
	logger.Println()
	logger.Printf("level %d has been sent for compaction...", levelNumber)

	if levelNumber == len(lsmtree.SSTableLevels)-1 {
		logger.Println("cannot compact last level")
		return nil
	}
	logger.Printf("number of ss tables in level %d are : %d", levelNumber, len(lsmtree.SSTableLevels[levelNumber].SSTables))

	sstables := make([]sst.SSTable, 0)
	lsmtree.SSTableLevels[levelNumber].RWMutex.Lock()

	if len(lsmtree.SSTableLevels[levelNumber].SSTables) < maxLevelMap[levelNumber] {
		logger.Printf("level %d only has %d ss tables, so no need for compaction", levelNumber, len(lsmtree.SSTableLevels[levelNumber].SSTables))
		lsmtree.SSTableLevels[levelNumber].RWMutex.Unlock()
		return nil
	}
	sstables = append(sstables, lsmtree.SSTableLevels[levelNumber].SSTables...)

	lsmtree.SSTableLevels[levelNumber].RWMutex.Unlock()

	var mergedKV []*proto_files.KeyValuePair

	if mergedKV, err = LinearMerge(sstables); err != nil {
		return err
	}

	if err = lsmtree.DeleteAllSSTablesInLevel(levelNumber); err != nil {
		return err
	}

	mergedSST, err := sst.SSTableInit(mergedKV, mergedKV[0].Key, mergedKV[len(mergedKV)-1].Key)

	if err != nil {
		return err
	}

	lsmtree.SSTableLevels[levelNumber].RWMutex.Lock()
	lsmtree.SSTableLevels[levelNumber+1].RWMutex.Lock()

	newSSTables := make([]sst.SSTable, 0)
	for i := 0; i < len(lsmtree.SSTableLevels[levelNumber].SSTables)-len(sstables); i++ {
		newSSTables = append(newSSTables, lsmtree.SSTableLevels[levelNumber].SSTables[i])
	}
	lsmtree.SSTableLevels[levelNumber].SSTables = newSSTables
	lsmtree.SSTableLevels[levelNumber+1].SSTables = append(lsmtree.SSTableLevels[levelNumber+1].SSTables, *mergedSST)
	logger.Println()
	logger.Printf("after compaction level %d now has %d sstables.. ", levelNumber+1, len(lsmtree.SSTableLevels[levelNumber+1].SSTables))
	logger.Println()

	lsmtree.SSTableLevels[levelNumber].RWMutex.Unlock()
	lsmtree.SSTableLevels[levelNumber+1].RWMutex.Unlock()

	if len(lsmtree.SSTableLevels[levelNumber+1].SSTables) > maxLevelMap[levelNumber+1] {
		lsmtree.compactLevelChannel <- levelNumber + 1
	}
	logger.Println()
	logger.Println("reading merged sstable..")

	if _, err = mergedSST.ReadSSTTable(); err != nil {
		return err
	}
	logger.Println()
	logger.Println("///////////////////////////////////////////////")
	logger.Println("/////////    COMPACTION PROCESS     ///////////")
	logger.Println("///////////////////////////////////////////////")
	logger.Println()
	return nil

}

func LinearMerge(sstables []sst.SSTable) (mergedKV []*proto_files.KeyValuePair, err error) {

	for i := len(sstables) - 1; i >= 0; i-- {

		var kv []*proto_files.KeyValuePair

		sstable := sstables[i]

		if kv, err = sstable.ReadSSTTable(); err != nil {
			return nil, err
		}
		mergedKV = sst.MergeDataBlock(kv, int64(i), mergedKV, math.MinInt64)
		logger.Printf("merged KV : %v", mergedKV)

	}

	return mergedKV, nil
}
func (lsmtree *LSMTree) DeleteAllSSTablesInLevel(levelNumber int) error {

	lsmtree.SSTableLevels[levelNumber].RWMutex.Lock()

	for _, sstable := range lsmtree.SSTableLevels[levelNumber].SSTables {

		if err := os.Remove(fmt.Sprintf("%s/%s", sst.SST_Directory, sstable.FileName)); err != nil {

			lsmtree.SSTableLevels[levelNumber].RWMutex.Unlock()
			return err
		}
	}
	lsmtree.SSTableLevels[levelNumber].RWMutex.Unlock()

	return nil
}
