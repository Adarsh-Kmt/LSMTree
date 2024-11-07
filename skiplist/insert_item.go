package skiplist

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"slices"
	"time"
)

type Node struct {
	key      int
	value    string
	next     []*Node
	maxLevel int
}

var (
	coin   = rand.New(rand.NewSource(time.Now().UnixNano()))
	logger = log.New(os.Stdout, "LSMTREE >> ", 0)
)

func isHead() bool {
	return coin.Intn(2) == 0
}

type SkipList struct {
	Sentinel       *Node
	numberOfLevels int
	maxLevel       int
	Tail           *Node
}

func SkipListInit(kv map[int]string) *SkipList {

	logger.Println("-------- INSERT SKIP LIST --------")

	sortedKeys := make([]int, len(kv))

	i := 0
	for key := range kv {
		sortedKeys[i] = key
		i++
	}

	slices.Sort(sortedKeys)

	sl := &SkipList{numberOfLevels: 16, maxLevel: 0}

	sentinel := &Node{key: math.MaxInt, value: "sentinel"}

	globalNext := make([]*Node, 16)

	for i := range globalNext {
		globalNext[i] = sentinel
	}

	sentinel.next = make([]*Node, 16)

	for _, key := range sortedKeys {

		value := kv[key]

		nd := &Node{key: key, value: value, next: make([]*Node, 16)}
		fmt.Printf("new node with key : %d value : %s created\n", nd.key, nd.value)
		currLevel := 0

		for isHead() {
			fmt.Printf("coin is heads, node with key %d initially at level %d increases to level %d\n", nd.key, currLevel, currLevel+1)
			globalNext[currLevel].next[currLevel] = nd
			globalNext[currLevel] = nd
			nd.maxLevel = currLevel
			if currLevel > sl.maxLevel {
				sl.maxLevel = currLevel
			}
			currLevel++

		}
		if currLevel == 0 {
			fmt.Printf("coin is heads, node with key %d remains at level %d\n", nd.key, currLevel)
			globalNext[currLevel].next[currLevel] = nd
			globalNext[currLevel] = nd
			nd.maxLevel = 0
		}
		fmt.Println()
	}

	logger.Println("-----------------------------------")

	sentinel.maxLevel = sl.maxLevel
	sl.Sentinel = sentinel

	return sl

}

func (sl *SkipList) DisplaySkipList() {

	currNode := sl.Sentinel

	logger.Println("-------- DISPLAY SKIP LIST --------")
	fmt.Printf("SkipList max level %d : \n", sl.maxLevel)
	for currNode != nil {
		fmt.Printf("key : %d value : %s maximum level reached : %d\n", currNode.key, currNode.value, currNode.maxLevel)
		currNode = currNode.next[0]
	}
	logger.Println("-----------------------------------")
}
