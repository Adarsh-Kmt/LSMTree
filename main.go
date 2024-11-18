package main

import (
	"bufio"
	"log"
	"os"
	"strconv"
	"strings"

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
		31: "a",
		32: "b",
		33: "c",
		34: "d",
		35: "e",
		36: "f",
		37: "g",
		38: "h",
		39: "i",
		40: "j",
		41: "k",
		42: "l",
		43: "m",
		44: "n",
		45: "o",
		46: "p",
		47: "q",
		48: "r",
		49: "s",
		50: "t",
		51: "u",
		52: "v",
		53: "w",
		54: "x",
		55: "y",
		56: "z",
		57: "aa",
		58: "ab",
		59: "ac",
		60: "ad",
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

	lsmtree := lsmtree.LSMTreeInit(3)

	lsmtree.LSMTreeSetup(kv)

	getInput(lsmtree)

}

func getInput(lsmtree *lsmtree.LSMTree) {

	scanner := bufio.NewReader(os.Stdin)

	for {
		logger.Print("enter input: ")
		input, err := scanner.ReadString('\n')
		input = strings.TrimRight(input, "\r\n")
		if err != nil {
			return
		}
		if input == "exit" {
			logger.Println("exiting...")
			return
		}

		logger.Print("1) enter 'insert' to insert item    \n2) enter 'search' to search for item    \n3) enter 'delete' to delete an item \n5) enter 'exit' to exit")

		option, err := scanner.ReadString('\n')
		if err != nil {
			continue
		}
		option = strings.TrimRight(option, "\r\n")

		if option == "exit" {
			return
		}

		logger.Print("enter key : ")
		keystr, err := scanner.ReadString('\n')
		if err != nil {
			return
		}
		keystr = strings.TrimRight(keystr, "\r\n")
		key, err := strconv.Atoi(keystr)

		if err != nil {
			return
		}

		if option == "insert" {

			logger.Print("enter value : ")
			value, err := scanner.ReadString('\n')
			value = strings.TrimRight(value, "\r\n")
			if err != nil {
				return
			}
			lsmtree.Put(key, value)

		} else if option == "search" {

			value, found := lsmtree.Get(key)
			if found {
				logger.Printf("value for key %d = %s", key, value)
			} else {
				logger.Printf("key %d not found in btree.", key)
			}
		} else if option == "delete" {

			err := lsmtree.Delete(key)

			if err != nil {
				logger.Printf(err.Error())
			} else {
				logger.Printf("key %d was deleted", key)

			}
		}
	}
}
