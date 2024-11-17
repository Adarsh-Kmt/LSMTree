package main

import (
	"log"
	"os"
	"time"

	"github.com/Adarsh-Kmt/LSMTree/lsmtree"
)

var (
	logger = log.New(os.Stdout, "LSMTREE >> ", 0)
	kv     = map[int]string{
		1:  "a",
		2:  "b",
		3:  "c",
		4:  "d",
		5:  "e",
		6:  "f",
		7:  "g",
		8:  "h",
		9:  "i",
		10: "j",
		11: "k",
		12: "l",
		13: "m",
		14: "n",
		15: "o",
		16: "p",
		17: "q",
		18: "r",
		19: "s",
		20: "t",
		21: "u",
		22: "v",
		23: "w",
		24: "x",
		25: "y",
		26: "z",
		27: "aa",
		28: "ab",
		29: "ac",
		30: "ad",
	}
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

	lsm := lsmtree.LSMTreeInit(2)

	for key, value := range kv {

		if err := lsm.Put(key, value); err != nil {
			logger.Printf("error : %s", err.Error())
			return
		}

	}
	//scanner := bufio.NewReader(os.Stdin)

	time.Sleep(1000 * time.Second)

	// testKeys := []int{5, 29, 26, 25}

	// lsm.Delete(5)
	// lsm.Delete(26)

	// for i := range testKeys {

	// 	if value, found := lsm.Get(testKeys[i]); found {
	// 		logger.Printf("found value %s for key %d", value, testKeys[i])
	// 	} else {
	// 		logger.Printf("didnt find a value for key %d", testKeys[i])
	// 	}
	// }

}
