package skiplist

import "fmt"

func (sl *SkipList) DeleteItem(key int) (value string, err error) {

	//predecessors := sl.getPredecessors(key)

	nd := sl.findItem(key)

	if nd == nil {
		return "", fmt.Errorf("node having key %d not found in skip list", key)
	}

	nd.tombStoneActivated = true

	return nd.value, nil
	// for index, predecessor := range predecessors {

	// 	if index > nd.maxLevel {
	// 		break
	// 	}
	// 	if predecessor != nil {
	// 		predecessor.next[index] = nd.next[index]
	// 	}
	// }

	// return nd.value, nil
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
