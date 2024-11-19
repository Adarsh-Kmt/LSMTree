package lsmtree

import (
	"context"
	"log"
	"os"
	"sync"

	memtable "github.com/Adarsh-Kmt/LSMTree/memtable"
	sst "github.com/Adarsh-Kmt/LSMTree/sstable"
)

var (
	maxLevelMap = map[int]int{
		0: 2,
		1: 1,
		2: 8,
		3: 16,
	}
	logFile, _ = os.OpenFile("write_log.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	writeLogger = log.New(logFile, "LSMTREE >> ", 0)
)

type LSMTree struct {
	currMEMTableRWMutex *sync.RWMutex
	currMEMTable        memtable.MEMTable
	SSTableLevels       []sst.SSTableLevel

	flushMEMTableQueue   []memtable.MEMTable
	flushMEMTableChannel chan struct{}
	flushMEMTableRWMutex *sync.RWMutex

	compactLevelChannel chan int
	ctx                 context.Context
	Wg                  sync.WaitGroup
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
		currMEMTableRWMutex: &sync.RWMutex{},

		flushMEMTableQueue:   make([]memtable.MEMTable, 0),
		flushMEMTableChannel: make(chan struct{}),
		flushMEMTableRWMutex: &sync.RWMutex{},

		SSTableLevels: sstLevels,

		compactLevelChannel: make(chan int, 100),

		ctx: context.Background(),
		Wg:  sync.WaitGroup{},
	}

	lsmtree.Wg.Add(2)
	go lsmtree.BackgroundCompaction()
	go lsmtree.BackgroundMEMTableFlush()
	return lsmtree
}

func (lsmtree *LSMTree) BackgroundMEMTableFlush() {

	for {

		select {

		case <-lsmtree.ctx.Done():

			lsmtree.flushMEMTableRWMutex.Lock()
			if len(lsmtree.flushMEMTableQueue) == 0 {
				return
			}
			lsmtree.flushMEMTableRWMutex.Unlock()

		case <-lsmtree.flushMEMTableChannel:
			if err := lsmtree.flushMEMTable(); err != nil {
				log.Printf("error while flushing MEMTable to disk : %s", err.Error())
				return
			}
		}
	}
}

func (lsmtree *LSMTree) flushMEMTable() (err error) {

	lsmtree.flushMEMTableRWMutex.Lock()
	memTable := lsmtree.flushMEMTableQueue[0]
	lsmtree.flushMEMTableQueue = lsmtree.flushMEMTableQueue[1:]
	lsmtree.flushMEMTableRWMutex.Unlock()

	var sstable *sst.SSTable
	if sstable, err = sst.SSTableInit(memTable.GetAllItems(), int64(memTable.GetMinKey()), int64(memTable.GetMaxKey())); err != nil {
		return err
	}

	lsmtree.SSTableLevels[0].RWMutex.Lock()
	lsmtree.SSTableLevels[0].SSTables = append(lsmtree.SSTableLevels[0].SSTables, *sstable)
	writeLogger.Println()
	writeLogger.Printf("level %d now has %d sstables...", 0, len(lsmtree.SSTableLevels[0].SSTables))
	writeLogger.Println()
	if len(lsmtree.SSTableLevels[0].SSTables) > maxLevelMap[0] {
		writeLogger.Println("pushing level 0 to compaction queue")
		lsmtree.compactLevelChannel <- 0
	}
	lsmtree.SSTableLevels[0].RWMutex.Unlock()

	return nil
}
func (lsmtree *LSMTree) Get(key int) (value string, found bool) {

	lsmtree.currMEMTableRWMutex.RLock()
	if value, found = lsmtree.currMEMTable.Get(key); found {

		lsmtree.currMEMTableRWMutex.RUnlock()
		if value == "tombstone" {
			return "", false
		}

		return value, found
	}

	lsmtree.currMEMTableRWMutex.RUnlock()

	for _, frozenMEMTable := range lsmtree.flushMEMTableQueue {

		if key >= frozenMEMTable.GetMinKey() && key <= frozenMEMTable.GetMaxKey() {

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

				level.RWMutex.RUnlock()
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

	lsmtree.currMEMTableRWMutex.Lock()
	lsmtree.currMEMTable.Put(key, value)
	lsmtree.currMEMTableRWMutex.Unlock()

	lsmtree.currMEMTable.Display()
	if lsmtree.currMEMTable.GetSize() >= 10 {

		lsmtree.flushMEMTableRWMutex.Lock()
		lsmtree.flushMEMTableQueue = append(lsmtree.flushMEMTableQueue, lsmtree.currMEMTable)
		lsmtree.flushMEMTableRWMutex.Unlock()

		lsmtree.flushMEMTableChannel <- struct{}{}
		// var sstable *sst.SSTable
		// if sstable, err = sst.SSTableInit(lsmtree.currMEMTable.GetAllItems(), int64(lsmtree.currMEMTable.GetMinKey()), int64(lsmtree.currMEMTable.GetMaxKey())); err != nil {
		// 	return err
		// }
		lsmtree.currMEMTableRWMutex.Lock()
		lsmtree.currMEMTable = memtable.SkipListInit(16)
		lsmtree.currMEMTableRWMutex.Unlock()
		// lsmtree.SSTableLevels[0].RWMutex.Lock()
		// lsmtree.SSTableLevels[0].SSTables = append(lsmtree.SSTableLevels[0].SSTables, *sstable)

		// lsmtree.SSTableLevels[0].RWMutex.Unlock()

		// if len(lsmtree.SSTableLevels[0].SSTables) > maxLevelMap[0] {
		// 	logger.Println("pushing level 0 to compaction queue")
		// 	lsmtree.compactLevelChannel <- 0
		// }
	}
	return nil
}

func (lsmtree *LSMTree) Delete(key int) (err error) {

	lsmtree.currMEMTableRWMutex.Lock()
	lsmtree.currMEMTable.Delete(key)
	lsmtree.currMEMTableRWMutex.Unlock()

	lsmtree.currMEMTable.Display()
	if lsmtree.currMEMTable.GetSize() >= 10 {

		lsmtree.flushMEMTableRWMutex.Lock()
		lsmtree.flushMEMTableQueue = append(lsmtree.flushMEMTableQueue, lsmtree.currMEMTable)
		lsmtree.flushMEMTableRWMutex.Unlock()

		lsmtree.flushMEMTableChannel <- struct{}{}
		// var sstable *sst.SSTable
		// if sstable, err = sst.SSTableInit(lsmtree.currMEMTable.GetAllItems(), int64(lsmtree.currMEMTable.GetMinKey()), int64(lsmtree.currMEMTable.GetMaxKey())); err != nil {
		// 	return err
		// }
		lsmtree.currMEMTableRWMutex.Lock()
		lsmtree.currMEMTable = memtable.SkipListInit(16)
		lsmtree.currMEMTableRWMutex.Unlock()
		// lsmtree.SSTableLevels[0].RWMutex.Lock()
		// lsmtree.SSTableLevels[0].SSTables = append(lsmtree.SSTableLevels[0].SSTables, *sstable)

		// lsmtree.SSTableLevels[0].RWMutex.Unlock()

		// if len(lsmtree.SSTableLevels[0].SSTables) > maxLevelMap[0] {
		// 	logger.Println("pushing level 0 to compaction queue")
		// 	lsmtree.compactLevelChannel <- 0
		// }

	}
	return nil
}

func (lsmtree *LSMTree) LSMTreeSetup(kv map[int]string) {
	for key, value := range kv {

		if err := lsmtree.Put(key, value); err != nil {
			log.Printf("error : %s", err.Error())
			return
		}
		//time.Sleep(500 * time.Millisecond)
	}
	log.Println("setup complete")
}
