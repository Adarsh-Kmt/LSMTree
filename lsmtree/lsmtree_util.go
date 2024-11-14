package lsmtree

// func (lsmtree *LSMTree) BackgroundCompaction() {

// 	for {

// 		select {

// 		case <-lsmtree.ctx.Done():

// 			if compactionTriggered := lsmtree.TriggerCompaction(); !compactionTriggered {
// 				return
// 			}
// 			// case index := <-lsmtree.CompactLevelChannel:

// 			// 	lsmtree.SSTableLevels[index].Compact()
// 		}
// 	}
// }

func (lsmtree *LSMTree) TriggerCompaction() (compactionTriggered bool) {

	compactionTriggered = false

	for index, level := range lsmtree.SSTableLevels {

		if index == len(lsmtree.SSTableLevels)-1 {
			continue
		}
		if level.NumberOfSSTables > level.MaxNumberOfSSTables {
			compactionTriggered = true
		}
	}

	return compactionTriggered
}
