package main

import (
	"log"
	"os"

	skiplist "github.com/Adarsh-Kmt/LSMTree/skiplist"
)

var (
	logger = log.New(os.Stdout, "LSMTREE >> ", 0)
)

func main() {

	kv := map[int]string{
		1: "a",
		2: "b",
		3: "c",
		4: "d",
		5: "e",
		6: "f",
		//7:  "g",
		8:  "h",
		9:  "i",
		10: "j",
		// 11: "k",
		// 12: "l",
		// 13: "m",
		// 14: "n",
		// 15: "o",
		// 16: "p",
		// 17: "q",
		// 18: "r",
		// 19: "s",
		// 20: "t",
		// 21: "u",
		// 22: "v",
		// 23: "w",
		// 24: "x",
		// 25: "y",
		// 26: "z",
	}

	sl := skiplist.SkipListInit(16)
	sl.SetupSkipList(kv)

	sl.DisplaySkipList()
	logger.Println()
	// value, found := sl.SearchItem(14)
	// if found {
	// 	logger.Printf("found key %d with value %s.", 14, value)
	// } else {
	// 	logger.Printf("did not find the key.")
	// }
	// logger.Println()
	// value, found = sl.SearchItem(8)
	// if found {
	// 	logger.Printf("found key %d with value %s.", 8, value)
	// } else {
	// 	logger.Printf("did not find the key.")
	// }

	sl.InsertItem(0, "o")
	sl.InsertItem(7, "o")
	sl.InsertItem(15, "o")
	sl.DeleteItem(4)
	// logger.Println()
	// sl.DisplaySkipList()
	// sl.DeleteItem(15)
	// logger.Println()
	// sl.DisplaySkipList()
	// sl.DeleteItem(8)

	if value, found := sl.SearchItem(4); !found {
		logger.Println("not found")
	} else {
		logger.Printf("value %s found", value)
	}
	logger.Println()
	sl.DisplaySkipList()
}
