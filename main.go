package main

import (
	"log"
	"os"

	"github.com/Adarsh-Kmt/LSMTree/lsmtree"
)

var (
	logger = log.New(os.Stdout, "LSMTREE >> ", 0)
)

// func main() {
// 	sl := memtable.SkipListInit(16)

// 	sl.SetupSkipList()

// 	sst, err := sstable.SSTableInit(sl)

// 	if err != nil {
// 		logger.Println(err.Error())
// 	}

// 	//err = sst.ReadSSTTable()

// 	if err != nil {
// 		logger.Println(err.Error())
// 	}

// 	if value, err := sst.Get(10); err != nil {
// 		logger.Printf("did not find key")
// 	} else {
// 		logger.Printf("found key with value %s", value)
// 	}

// }

func main() {

	lsm := lsmtree.LSMTreeInit(1)

	keys := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26}
	values := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"}

	for i := range values {

		if err := lsm.Put(keys[i], values[i]); err != nil {
			logger.Printf("error : %s", err.Error())
			return
		}

	}

	testKeys := []int{1, 4, 5, 27, 23, 29, 26, 25, 30}

	for i := range testKeys {

		if value, found := lsm.Get(testKeys[i]); found {
			logger.Printf("found value %s for key %d", value, testKeys[i])
		} else {
			logger.Printf("didnt find a value for key %d", testKeys[i])
		}
	}
}
