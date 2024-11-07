package skiplist

import "fmt"

func (sl *SkipList) SearchItem(key int) (value string, found bool) {

	logger.Println("-------- SEARCH SKIP LIST --------")

	currNode := sl.Sentinel

	for currNode != nil {

		fmt.Printf("currently at node with key %d \n", currNode.key)
		if currNode.key == key {
			logger.Println("-----------------------------------")
			return currNode.value, true

		}
		currLevel := currNode.maxLevel
		nextNode := currNode.next[currLevel]

		for currLevel > 0 && (nextNode == nil || nextNode.key > key) {
			fmt.Printf("currently at level %d\n", currLevel)
			currLevel--
			nextNode = currNode.next[currLevel]
		}

		currNode = nextNode
		fmt.Println()
	}

	logger.Println("-----------------------------------")

	return "", false
}
