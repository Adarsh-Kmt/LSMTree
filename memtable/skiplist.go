package memtable

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"time"

	"github.com/Adarsh-Kmt/LSMTree/proto_files"
)

var (
	coin = rand.New(rand.NewSource(time.Now().UnixNano()))
	//logFile, _ = os.OpenFile("log_file.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	logger = log.New(os.Stdout, "LSMTREE >> ", 0)
)

type MEMTable interface {
	Put(key int, value string)
	Get(key int) (value string, found bool)
	Delete(key int)
	GetAllItems() (kv []*proto_files.KeyValuePair)
	GetMinKey() (key int)
	GetMaxKey() (key int)
}

type Node struct {
	key      int
	value    string
	next     []*Node
	maxLevel int
}

type SkipList struct {
	Sentinel   *Node
	globalNext []*Node

	numberOfLevels int
	maxLevel       int

	MaxKey int
	MinKey int

	Size int
}

func isHead() bool {
	return coin.Intn(2) == 0
}

func SkipListInit(numberOfLevels int) *SkipList {

	sentinel := &Node{
		key:   math.MinInt,
		value: "sentinel",
		next:  make([]*Node, numberOfLevels),
	}

	sl := &SkipList{
		numberOfLevels: numberOfLevels,
		maxLevel:       0,

		MinKey: math.MaxInt,
		MaxKey: math.MinInt,

		Size:       0,
		Sentinel:   sentinel,
		globalNext: make([]*Node, numberOfLevels),
	}

	for i := 0; i < numberOfLevels; i++ {
		sl.globalNext[i] = sentinel
	}

	return sl

}

func (sl *SkipList) Put(key int, value string) {

	sl.Size++
	predecessors := sl.getPredecessors(key)

	newNode := &Node{
		key:   key,
		value: value,
		next:  make([]*Node, sl.numberOfLevels),
	}

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

		if sl.MaxKey < key {
			sl.MaxKey = key
		}

		if sl.MinKey > key {
			sl.MinKey = key
		}
	}
}

func (sl *SkipList) Get(key int) (value string, found bool) {

	//logger.Println("-------- SEARCH SKIP LIST --------")

	currNode := sl.Sentinel

	for currNode != nil {

		//fmt.Printf("currently at node with key %d \n", currNode.key)
		if currNode.key == key {

			//logger.Println("-----------------------------------")
			return currNode.value, true

		}
		currLevel := currNode.maxLevel
		nextNode := currNode.next[currLevel]

		for currLevel > 0 && (nextNode == nil || nextNode.key > key) {
			//fmt.Printf("currently at level %d\n", currLevel)
			currLevel--
			nextNode = currNode.next[currLevel]
		}

		currNode = nextNode
		fmt.Println()
	}

	//logger.Println("-----------------------------------")

	return "", false
}

func (sl *SkipList) Delete(key int) {

	nd := sl.findItem(key)

	if nd == nil {
		sl.Put(key, "tombstone")
	} else {
		nd.value = "tombstone"
	}

}

func (sl *SkipList) GetAllItems() (kv []*proto_files.KeyValuePair) {

	kv = make([]*proto_files.KeyValuePair, 0)

	currNode := sl.Sentinel.next[0]

	for currNode != nil {

		kv = append(kv, &proto_files.KeyValuePair{Key: int64(currNode.key), Value: currNode.value})
		currNode = currNode.next[0]
	}

	return kv
}

func (sl *SkipList) GetMaxKey() (key int) {
	return sl.MaxKey
}

func (sl *SkipList) GetMinKey() (key int) {
	return sl.MinKey
}
