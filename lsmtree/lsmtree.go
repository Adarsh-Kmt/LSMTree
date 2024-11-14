package lsmtree

import (
	memtable "github.com/Adarsh-Kmt/LSMTree/memtable"
	sst "github.com/Adarsh-Kmt/LSMTree/sstable"
)

// const (
// 	ssTableDirectory = "sstable_dir"
// )

type LSMTree struct {
	currMEMTable          *memtable.SkipList
	frozenMEMTables       []*memtable.SkipList
	numberOfSSTableLevels int
	SSTableLevels         []sst.SSTableLevel
	FlushMEMTableChannel  <-chan memtable.MEMTable
	CompactLevelChannel   <-chan int
}

func LSMTreeInit(numberOfSSTableLevels int) *LSMTree {

	sstLevels := make([]sst.SSTableLevel, 0)
	sstLevels = append(sstLevels, sst.SSTableLevel{LevelNumber: 0, NumberOfSSTables: 0, SSTables: make([]sst.SSTable, 0), MaxNumberOfSSTables: 5})
	return &LSMTree{

		currMEMTable:          memtable.SkipListInit(16),
		frozenMEMTables:       make([]*memtable.SkipList, 0),
		numberOfSSTableLevels: numberOfSSTableLevels,
		SSTableLevels:         sstLevels,
	}
}

func (lsmtree *LSMTree) Get(key int) (value string, found bool) {

	if value, found = lsmtree.currMEMTable.Get(key); found {
		return value, found
	}

	for _, frozenMEMTable := range lsmtree.frozenMEMTables {

		if key >= frozenMEMTable.MinKey && key <= frozenMEMTable.MaxKey {

			if value, found = frozenMEMTable.Get(key); found {
				return value, found
			}
		}
	}

	for _, level := range lsmtree.SSTableLevels {

		for i := len(level.SSTables) - 1; i >= 0; i-- {

			sstable := level.SSTables[i]
			var err error
			if value, err = sstable.Get(int64(key)); err == nil {
				return value, true
			}
		}
	}
	return "", false
}

func (lsmtree *LSMTree) Put(key int, value string) (err error) {

	lsmtree.currMEMTable.Put(key, value)
	lsmtree.currMEMTable.DisplaySkipList()
	if lsmtree.currMEMTable.Size >= 10 {

		var sstable *sst.SSTable
		if sstable, err = sst.SSTableInit(lsmtree.currMEMTable); err != nil {
			return err
		}
		lsmtree.currMEMTable = memtable.SkipListInit(16)

		lsmtree.SSTableLevels[0].SSTables = append(lsmtree.SSTableLevels[0].SSTables, *sstable)

	}
	return nil
}
