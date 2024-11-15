package memtable

import (
	"fmt"
	"slices"
)

func (sl *SkipList) SetupSkipList() {

	kv := map[int]string{
		1:  "adarsh",
		2:  "kamath",
		3:  "built",
		4:  "a",
		5:  "log",
		6:  "structured",
		7:  "merge",
		8:  "tree",
		9:  "on",
		10: "his",
		11: "own",
		12: "for",
		13: "fun",
		14: "!",
		15: "check",
		16: "it",
		17: "out",
		18: "now",
		19: "!",
	}

	sortedKeys := make([]int, len(kv))

	i := 0
	for key := range kv {
		sortedKeys[i] = key
		i++
	}

	slices.Sort(sortedKeys)

	for _, key := range sortedKeys {
		sl.Put(key, kv[key])
	}

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

	return predecessors
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

func (sl *SkipList) findItem(key int) (nd *Node) {

	//logger.Println("-------- SEARCH SKIP LIST --------")

	currNode := sl.Sentinel

	for currNode != nil {

		//fmt.Printf("currently at node with key %d \n", currNode.key)
		if currNode.key == key {
			//logger.Println("-----------------------------------")
			return currNode

		}
		currLevel := currNode.maxLevel
		nextNode := currNode.next[currLevel]

		for currLevel > 0 && (nextNode == nil || nextNode.key > key) {
			//fmt.Printf("currently at level %d\n", currLevel)
			currLevel--
			nextNode = currNode.next[currLevel]
		}

		currNode = nextNode
		//fmt.Println()
	}

	//logger.Println("-----------------------------------")

	return nil
}
