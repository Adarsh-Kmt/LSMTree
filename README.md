# LSMTree
A LSM tree, implemented in Go.

## Format of SSTable 

|   Block Name    |
|:---------------:|
|  Header Block   |
|   Data Block 1  |
|   Data Block 2  |
|        .        |
|        .        |
| Data Block N-1  |
|  Data Block N   |
|   Index Block   |
| Meta Data Block |


- Format of Header Block