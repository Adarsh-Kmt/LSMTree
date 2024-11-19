package lsmtree

import (
	"fmt"
	"log"
	"math"
	"os"

	"github.com/Adarsh-Kmt/LSMTree/proto_files"
	sst "github.com/Adarsh-Kmt/LSMTree/sstable"
)

var (
	compactionLogFile, _ = os.OpenFile("log_files/compaction_log.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	compactionLogger     = log.New(compactionLogFile, "LSMTREE >> ", 0)
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

				compactionLogger.Printf("error during compaction : %s", err.Error())
				return
			}
		}
	}
}

func (lsmtree *LSMTree) TriggerCompaction() (compactionTriggered bool) {

	compactionTriggered = false

	for index, level := range lsmtree.SSTableLevels {

		level.RWMutex.Lock()
		if index == len(lsmtree.SSTableLevels)-1 {
			level.RWMutex.Unlock()
			continue
		}
		if level.NumberOfSSTables > level.MaxNumberOfSSTables {
			compactionTriggered = true
			level.RWMutex.Unlock()
			lsmtree.SizeTieredCompaction(index)
		}
		level.RWMutex.Unlock()
	}

	return compactionTriggered
}

func (lsmtree *LSMTree) SizeTieredCompaction(levelNumber int) (err error) {

	compactionLogger.Println("///////////////////////////////////////////////")
	compactionLogger.Println("/////////    COMPACTION PROCESS     ///////////")
	compactionLogger.Println("///////////////////////////////////////////////")
	compactionLogger.Println()
	compactionLogger.Printf("level %d has been sent for compaction...", levelNumber)

	lsmtree.SSTableLevels[levelNumber].RWMutex.Lock()

	if levelNumber == len(lsmtree.SSTableLevels)-1 {
		compactionLogger.Println("cannot compact last level")
		return nil
	}

	compactionLogger.Printf("number of ss tables in level %d are : %d", levelNumber, len(lsmtree.SSTableLevels[levelNumber].SSTables))

	sstables := make([]sst.SSTable, 0)

	if len(lsmtree.SSTableLevels[levelNumber].SSTables) < maxLevelMap[levelNumber] {
		compactionLogger.Printf("level %d only has %d ss tables, so no need for compaction", levelNumber, len(lsmtree.SSTableLevels[levelNumber].SSTables))

		lsmtree.SSTableLevels[levelNumber].RWMutex.Unlock()
		return nil
	}
	sstables = append(sstables, lsmtree.SSTableLevels[levelNumber].SSTables...)
	compactionLogger.Println("merging files => ")
	for _, sstable := range sstables {
		compactionLogger.Printf("file %s", sstable.FileName)
	}
	lsmtree.SSTableLevels[levelNumber].RWMutex.Unlock()

	var mergedKV []*proto_files.KeyValuePair

	if mergedKV, err = LinearMerge(sstables); err != nil {
		return err
	}

	mergedSST, err := sst.SSTableInit(mergedKV, mergedKV[0].Key, mergedKV[len(mergedKV)-1].Key)

	if err != nil {
		return err
	}

	lsmtree.SSTableLevels[levelNumber].RWMutex.Lock()
	lsmtree.SSTableLevels[levelNumber+1].RWMutex.Lock()

	if err = lsmtree.DeleteAllSSTablesInLevel(levelNumber, len(sstables)); err != nil {
		return err
	}

	// for i := 0; i < len(lsmtree.SSTableLevels[levelNumber].SSTables)-len(sstables); i++ {
	// 	newSSTables = append(newSSTables, lsmtree.SSTableLevels[levelNumber].SSTables[i])
	// }
	lsmtree.SSTableLevels[levelNumber].SSTables = lsmtree.SSTableLevels[levelNumber].SSTables[len(sstables):]
	lsmtree.SSTableLevels[levelNumber+1].SSTables = append(lsmtree.SSTableLevels[levelNumber+1].SSTables, *mergedSST)
	compactionLogger.Println()
	compactionLogger.Printf("after compaction level %d now has %d sstables.. ", levelNumber+1, len(lsmtree.SSTableLevels[levelNumber+1].SSTables))
	for _, sstable := range lsmtree.SSTableLevels[levelNumber+1].SSTables {
		compactionLogger.Printf("file : %s", sstable.FileName)
	}
	compactionLogger.Println()

	if len(lsmtree.SSTableLevels[levelNumber+1].SSTables) > maxLevelMap[levelNumber+1] {
		lsmtree.compactLevelChannel <- levelNumber + 1
	}

	lsmtree.SSTableLevels[levelNumber].RWMutex.Unlock()
	lsmtree.SSTableLevels[levelNumber+1].RWMutex.Unlock()

	compactionLogger.Println()
	compactionLogger.Println("reading merged sstable..")

	if _, err = mergedSST.ReadSSTTable(); err != nil {
		return err
	}
	compactionLogger.Println()
	compactionLogger.Println("///////////////////////////////////////////////")
	compactionLogger.Println("/////////    COMPACTION PROCESS     ///////////")
	compactionLogger.Println("///////////////////////////////////////////////")
	compactionLogger.Println()
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
		compactionLogger.Printf("merged KV : %v", mergedKV)

	}

	return mergedKV, nil
}
func (lsmtree *LSMTree) DeleteAllSSTablesInLevel(levelNumber int, numberOfSSTablesToDelete int) error {

	//lsmtree.SSTableLevels[levelNumber].RWMutex.Lock()

	for i := 0; i < numberOfSSTablesToDelete; i++ {
		sstable := lsmtree.SSTableLevels[levelNumber].SSTables[i]

		compactionLogger.Printf("deleting file %s from level %d", sstable.FileName, levelNumber)
		if err := os.Remove(fmt.Sprintf("%s/%s", sst.SST_Directory, sstable.FileName)); err != nil {

			lsmtree.SSTableLevels[levelNumber].RWMutex.Unlock()
			return err
		}
	}

	//lsmtree.SSTableLevels[levelNumber].RWMutex.Unlock()

	return nil
}
