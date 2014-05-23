/*
PriorityQueue implementation take from golang std library container/heap documentation example
*/
package host

// PriorityQueue implements the heap.Interface and holds hostitems
type PriorityQueue []*Item

// hostitem is what is stored in the least commited RAM scheduler's priority queue
type Item struct {
	Host     *Host
	Priority uint64 // the host's available RAM
	Index    int    // the index of the hostitem in the heap
}

// Len is the number of elements in the collection.
func (pq PriorityQueue) Len() int {
	return len(pq)
}

// Less reports whether the element with index i should sort before the element with index j.
func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Priority > pq[j].Priority
}

// Swap swaps the elements with indexes i and j.
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

// Push pushes the hostitem onto the heap.
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.Index = n
	*pq = append(*pq, item)
}

// Pop removes the minimum element (according to Less) from the heap and returns it.
func (pq *PriorityQueue) Pop() interface{} {
	opq := *pq
	n := len(opq)
	item := opq[n-1]
	item.Index = -1 // mark it as removed, just in case
	*pq = opq[0 : n-1]
	return item
}
