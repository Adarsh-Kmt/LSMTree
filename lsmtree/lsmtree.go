package lsmtree

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	memtable "github.com/Adarsh-Kmt/LSMTree/memtable"
	sst "github.com/Adarsh-Kmt/LSMTree/sstable"
)

var (
	maxLevelMap = map[int]int{
		0: 2,
		1: 4,
		2: 8,
		3: 16,
	}
	///logFile, _ = os.OpenFile("log_file.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	logger = log.New(os.Stdout, "LSMTREE >> ", 0)
)

type LSMTree struct {
	RWMMutex        *sync.RWMutex
	currMEMTable    *memtable.SkipList
	frozenMEMTables []*memtable.SkipList
	SSTableLevels   []sst.SSTableLevel
	//flushMEMTableChannel <-chan memtable.MEMTable
	compactLevelChannel chan int

	ctx context.Context
	Wg  sync.WaitGroup
}

func LSMTreeInit(numberOfSSTableLevels int) *LSMTree {

	sstLevels := make([]sst.SSTableLevel, 0)

	for i := 0; i < numberOfSSTableLevels; i++ {

		sstLevels = append(sstLevels, sst.SSTableLevel{
			LevelNumber:         i,
			NumberOfSSTables:    0,
			SSTables:            make([]sst.SSTable, 0),
			MaxNumberOfSSTables: maxLevelMap[i],
			RWMutex:             &sync.RWMutex{},
		})
	}

	//sstLevels = append(sstLevels, sst.SSTableLevel{LevelNumber: 0, NumberOfSSTables: 0, SSTables: make([]sst.SSTable, 0), MaxNumberOfSSTables: 5})
	lsmtree := &LSMTree{

		currMEMTable:        memtable.SkipListInit(16),
		frozenMEMTables:     make([]*memtable.SkipList, 0),
		SSTableLevels:       sstLevels,
		compactLevelChannel: make(chan int, 100),
		ctx:                 context.Background(),
		Wg:                  sync.WaitGroup{},
		RWMMutex:            &sync.RWMutex{},
	}

	lsmtree.Wg.Add(1)
	go lsmtree.BackgroundCompaction()

	return lsmtree
}

func (lsmtree *LSMTree) Get(key int) (value string, found bool) {

	lsmtree.RWMMutex.RLock()
	if value, found = lsmtree.currMEMTable.Get(key); found {

		lsmtree.RWMMutex.RUnlock()
		if value == "tombstone" {
			return "", false
		}

		return value, found
	}

	lsmtree.RWMMutex.RUnlock()

	for _, frozenMEMTable := range lsmtree.frozenMEMTables {

		if key >= frozenMEMTable.MinKey && key <= frozenMEMTable.MaxKey {

			if value, found = frozenMEMTable.Get(key); found {

				if value == "tombstone" {
					return "", false
				}

				return value, found
			}
		}
	}

	for _, level := range lsmtree.SSTableLevels {

		level.RWMutex.RLock()
		for i := len(level.SSTables) - 1; i >= 0; i-- {

			sstable := level.SSTables[i]
			var err error
			if value, err = sstable.Get(int64(key)); err == nil {

				if value == "tombstone" {
					return "", false
				}
				return value, true
			}
		}
		level.RWMutex.RUnlock()
	}
	return "", false
}

func (lsmtree *LSMTree) Put(key int, value string) (err error) {

	lsmtree.RWMMutex.Lock()
	lsmtree.currMEMTable.Put(key, value)
	lsmtree.RWMMutex.Unlock()

	lsmtree.currMEMTable.DisplaySkipList()
	if lsmtree.currMEMTable.Size >= 10 {

		var sstable *sst.SSTable
		if sstable, err = sst.SSTableInit(lsmtree.currMEMTable.GetAllItems(), int64(lsmtree.currMEMTable.MinKey), int64(lsmtree.currMEMTable.MaxKey)); err != nil {
			return err
		}
		lsmtree.currMEMTable = memtable.SkipListInit(16)

		lsmtree.SSTableLevels[0].RWMutex.Lock()
		lsmtree.SSTableLevels[0].SSTables = append(lsmtree.SSTableLevels[0].SSTables, *sstable)
		logger.Println()
		logger.Printf("level %d now has %d sstables...", 0, len(lsmtree.SSTableLevels[0].SSTables))
		logger.Println()
		lsmtree.SSTableLevels[0].RWMutex.Unlock()

		if len(lsmtree.SSTableLevels[0].SSTables) > maxLevelMap[0] {
			logger.Println("pushing level 0 to compaction queue")
			lsmtree.compactLevelChannel <- 0
		}
	}
	return nil
}

func (lsmtree *LSMTree) Delete(key int) (err error) {

	lsmtree.RWMMutex.Lock()
	lsmtree.currMEMTable.Delete(key)
	lsmtree.RWMMutex.Unlock()

	lsmtree.currMEMTable.DisplaySkipList()
	if lsmtree.currMEMTable.Size >= 10 {

		var sstable *sst.SSTable
		if sstable, err = sst.SSTableInit(lsmtree.currMEMTable.GetAllItems(), int64(lsmtree.currMEMTable.MinKey), int64(lsmtree.currMEMTable.MaxKey)); err != nil {
			return err
		}
		lsmtree.currMEMTable = memtable.SkipListInit(16)

		lsmtree.SSTableLevels[0].RWMutex.Lock()
		lsmtree.SSTableLevels[0].SSTables = append(lsmtree.SSTableLevels[0].SSTables, *sstable)
		logger.Println()
		logger.Printf("level %d now has %d sstables...", 0, len(lsmtree.SSTableLevels[0].SSTables))
		logger.Println()
		lsmtree.SSTableLevels[0].RWMutex.Unlock()

		if len(lsmtree.SSTableLevels[0].SSTables) > maxLevelMap[0] {
			logger.Println("pushing level 0 to compaction queue")
			lsmtree.compactLevelChannel <- 0
		}

	}
	return nil
}

func (lsmtree *LSMTree) LSMTreeSetup(kv map[int]string) {
	for key, value := range kv {

		if err := lsmtree.Put(key, value); err != nil {
			logger.Printf("error : %s", err.Error())
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	logger.Println("setup complete")
}
