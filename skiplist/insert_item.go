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
	key                int
	value              string
	next               []*Node
	maxLevel           int
	tombStoneActivated bool
}

type SkipList struct {
	Sentinel   *Node
	globalNext []*Node

	numberOfLevels int
	maxLevel       int

	MaxKey int
	MinKey int

	IsFrozen bool

	Size int
}

var (
	coin   = rand.New(rand.NewSource(time.Now().UnixNano()))
	logger = log.New(os.Stdout, "LSMTREE >> ", 0)
)

func isHead() bool {
	return coin.Intn(2) == 0
}

func (sl *SkipList) DisplaySkipList() {

	currNode := sl.Sentinel

	logger.Println("-------- DISPLAY SKIP LIST --------")
	fmt.Printf("SkipList max level %d : \n", sl.maxLevel)
	for currNode != nil {
		fmt.Printf("key : %d value : %s  tombstone : %t maximum level reached : %d\n", currNode.key, currNode.value, currNode.tombStoneActivated, currNode.maxLevel)
		currNode = currNode.next[0]
	}
	logger.Println("-----------------------------------")
}

func (sl *SkipList) getPredecessors(key int) (predecessors []*Node) {

	predecessors = make([]*Node, sl.numberOfLevels)

	for i := 0; i < len(predecessors); i++ {
		predecessors[i] = sl.Sentinel
	}

	currNode := sl.Sentinel

	for currNode != nil {

		if currNode.key >= key {
			break
		}

		currLevel := currNode.maxLevel

		for i := 0; i <= currLevel; i++ {
			predecessors[i] = currNode
		}
		nextNode := currNode.next[currNode.maxLevel]

		for currLevel > 0 && (nextNode == nil || nextNode.key >= key) {
			currLevel--
			nextNode = currNode.next[currLevel]
		}
		currNode = nextNode
	}

	// for index, predecessorNode := range predecessors {
	// 	fmt.Printf("predecessor node at level %d is => key : %d\n", index, predecessorNode.key)
	// }
	// fmt.Println()

	return predecessors
}

func (sl *SkipList) InsertItem(key int, value string) {

	sl.Size++
	predecessors := sl.getPredecessors(key)

	newNode := &Node{
		key:                key,
		value:              value,
		next:               make([]*Node, sl.numberOfLevels),
		tombStoneActivated: false}

	maxLevel := 0

	for isHead() {
		maxLevel++
	}

	newNode.maxLevel = maxLevel

	for i := 0; i <= maxLevel; i++ {

		if predecessors[i] != nil {
			newNode.next[i] = predecessors[i].next[i]
			predecessors[i].next[i] = newNode

		} else {

			currPredecessor := predecessors[i]

			for j := i + 1; j < len(predecessors) && currPredecessor == nil; j++ {
				currPredecessor = predecessors[j]
			}

			if currPredecessor != nil {

				newNode.next[i] = currPredecessor.next[i]
				currPredecessor.next[i] = newNode
			} else {
				// new node has a level greater than the greatest level currently seen by the skip list
				sl.maxLevel = i
			}
		}
	}
}

func SkipListInit(numberOfLevels int) *SkipList {

	sentinel := &Node{
		key:                math.MinInt,
		value:              "sentinel",
		tombStoneActivated: false,
		next:               make([]*Node, numberOfLevels),
	}

	sl := &SkipList{
		numberOfLevels: numberOfLevels,
		maxLevel:       0,

		MinKey: math.MaxInt,
		MaxKey: math.MinInt,

		IsFrozen: false,

		Size:       0,
		Sentinel:   sentinel,
		globalNext: make([]*Node, numberOfLevels),
	}

	for i := 0; i < numberOfLevels; i++ {
		sl.globalNext[i] = sentinel
	}

	return sl

}

func (sl *SkipList) SetupSkipList(kv map[int]string) {

	sortedKeys := make([]int, len(kv))

	i := 0
	for key := range kv {
		sortedKeys[i] = key
		i++
	}

	slices.Sort(sortedKeys)

	for _, key := range sortedKeys {
		sl.InsertItem(key, kv[key])
	}

}

func (sl *SkipList) BatchInsertItem(kv map[int]string) {

	sortedKeys := make([]int, len(kv))

	i := 0
	for key := range kv {
		sortedKeys[i] = key
		i++
	}

	slices.Sort(sortedKeys)

	if test := true; test {

		for _, key := range sortedKeys {
			sl.InsertItem(key, kv[key])
		}
		return
	}

	for _, key := range sortedKeys {

		if key < sl.MinKey {
			sl.MinKey = key
		}

		if key > sl.MaxKey {
			sl.MaxKey = key
		}
		value := kv[key]

		nd := &Node{key: key,
			value:              value,
			next:               make([]*Node, 16),
			tombStoneActivated: false}

		//fmt.Printf("new node with key : %d value : %s created\n", nd.key, nd.value)
		currLevel := 0

		for isHead() {
			//fmt.Printf("coin is heads, node with key %d initially at level %d increases to level %d\n", nd.key, currLevel, currLevel+1)
			sl.globalNext[currLevel].next[currLevel] = nd
			sl.globalNext[currLevel] = nd
			nd.maxLevel = currLevel
			if currLevel > sl.maxLevel {
				sl.maxLevel = currLevel
			}
			currLevel++

		}
		if currLevel == 0 {
			//fmt.Printf("coin is heads, node with key %d remains at level %d\n", nd.key, currLevel)
			sl.globalNext[currLevel].next[currLevel] = nd
			sl.globalNext[currLevel] = nd
			nd.maxLevel = 0
		}
		//fmt.Println()
	}

	//logger.Println("-----------------------------------")

	sl.Sentinel.maxLevel = sl.maxLevel

}
