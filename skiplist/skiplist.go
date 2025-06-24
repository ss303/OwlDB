package skiplist

import (
	"cmp"
	"fmt"
	"log/slog"
	"math/rand"
	"reflect"
	"sync/atomic"
)

// SkipList is the main structure that holds the data in a probabilistic balanced tree.
type SkipList[K cmp.Ordered, V any] struct {
	maxLevel int
	head     *SkipNode[K, V]
	tail     *SkipNode[K, V]
	opCount  atomic.Uint64
	maxKey   K
	minKey   K
}

// randomLevel generates a random level for a new node.
// Input: None
// Output: Random level as an integer
func (skipList *SkipList[K, V]) randomLevel() int {
	level := 0
	for level < skipList.maxLevel-1 && rand.Intn(2) == 0 {
		level++
	}
	return level
}

// NewSkipList initializes a new SkipList with a given maximum level and sentinel keys for head and tail.
// Input: Maximum level (int), minimum key (K), maximum key (K)
// Output: Pointer to SkipList
func NewSkipList[K cmp.Ordered, V any](maxLevel int, minKey K, maxKey K) *SkipList[K, V] {
	headNode := InitializeNode[K, V](minKey, nil, maxLevel)
	tailNode := InitializeNode[K, V](maxKey, nil, maxLevel)
	for i := range headNode.nextNodes {
		headNode.nextNodes[i].Store(tailNode)
	}

	return &SkipList[K, V]{
		maxLevel: maxLevel,
		head:     headNode,
		tail:     tailNode,
		maxKey:   maxKey,
		minKey:   minKey,
	}
}

// UpdateCheck defines a function signature for checking and updating values in the skip list.
type UpdateCheck[K cmp.Ordered, V any] func(key K, currentValue *V, exists bool) (newValue *V, err error)

// CopyFunc defines a function signature for creating a deep copy of a value.
type CopyFunc[K cmp.Ordered, V any] func(currentValue *V) (deepCopy *V, err error)

// GetCopy retrieves a deep copy of a value associated with a given key.
// Input: Key (K), Copy function (CopyFunc)
// Output: Pointer to the copied value (deepCopy), error if any
func (skipList *SkipList[K, V]) GetCopy(key K, copyFunction CopyFunc[K, V]) (deepCopy *V, err error) {
	for {
		if key <= skipList.minKey || key >= skipList.maxKey {
			return nil, fmt.Errorf("invalid key")
		}
		levelFound, _, successors := skipList.find(key)

		exists := levelFound != -1
		if !exists {
			return nil, fmt.Errorf("document does not exist")
		}

		nodeFound := successors[levelFound].Load()
		nodeFound.mu.Lock()
		nodeValue := nodeFound.nodeValue.Load()

		// Check if things have changed since finding the node
		if nodeFound.isMarked.Load() || !nodeFound.isFullyLinked.Load() || nodeFound.maxLevel != levelFound {
			nodeFound.mu.Unlock()
			continue
		}

		valueCopy, err := copyFunction(nodeValue)
		if err != nil {
			nodeFound.mu.Unlock()
			return nil, err
		}

		nodeFound.mu.Unlock()
		return valueCopy, nil
	}
}

// Upsert inserts a new key-value pair or updates the value if the key already exists.
// Input: Key (K), Update check function (UpdateCheck)
// Output: Boolean indicating if updated (updated), error if any
func (skipList *SkipList[K, V]) Upsert(key K, check UpdateCheck[K, V]) (updated bool, err error) {
	for {
		if key <= skipList.minKey || key >= skipList.maxKey {
			return false, fmt.Errorf("invalid key")
		}
		levelFound, predecessors, successors := skipList.find(key)

		exists := levelFound != -1
		var nodeFound *SkipNode[K, V]
		var nodeValue *V
		var topLevel int
		if exists {
			nodeFound = successors[levelFound].Load()
			topLevel = nodeFound.maxLevel
			nodeFound.mu.Lock()
			nodeValue = nodeFound.nodeValue.Load()

			// Check if things have changed since finding the node
			if nodeFound.isMarked.Load() || !nodeFound.isFullyLinked.Load() || nodeFound.maxLevel != levelFound {
				nodeFound.mu.Unlock()
				continue
			}
		} else {
			nodeFound = nil
			topLevel = skipList.randomLevel()
			nodeValue = nil
		}

		valid := true
		level := 0
		uniquePredecessorsLocked := make(map[*SkipNode[K, V]]bool, len(predecessors))

		// Lock unique predecessors
		for valid && level <= topLevel {
			predecessor := predecessors[level].Load()
			_, locked := uniquePredecessorsLocked[predecessor]
			if !locked {
				predecessor.mu.Lock()
				uniquePredecessorsLocked[predecessor] = true
			}

			unmarked := !predecessors[level].Load().isMarked.Load() && !successors[level].Load().isMarked.Load()
			connected := predecessors[level].Load().nextNodes[level].Load() == successors[level].Load()
			valid = unmarked && connected

			level++
		}

		slog.Info("Locked all predecessors")

		if !valid {
			slog.Info("Another node locked predecessors")
			for predecessor := range uniquePredecessorsLocked {
				predecessor.mu.Unlock()
			}
			if exists {
				nodeFound.mu.Unlock()
			}
			continue
		}

		// Execute the update check function
		returnValue, err := check(key, nodeValue, exists)

		slog.Info("Got past check")

		if err != nil {
			for predecessor := range uniquePredecessorsLocked {
				predecessor.mu.Unlock()
			}
			if exists {
				nodeFound.mu.Unlock()
			}
			return true, err
		}

		updated = true
		if returnValue != nil {
			updated = false
			slog.Info("creating new node")
			newNode := InitializeNode(key, returnValue, topLevel)

			slog.Info("inserting node")
			level := 0
			for level <= newNode.maxLevel {
				newNode.nextNodes[level].Store(successors[level].Load())
				level++
			}

			level = 0
			for level <= newNode.maxLevel {
				predecessors[level].Load().nextNodes[level].Store(newNode)
				level++
			}

			newNode.isFullyLinked.Store(true)
		} else {
			slog.Info("updated node")
			nodeFound.mu.Unlock()
		}

		for predecessor := range uniquePredecessorsLocked {
			predecessor.mu.Unlock()
		}

		skipList.opCount.Add(1)
		return updated, nil
	}
}

// QueryCopies retrieves pointers to copies of values between the given keys.
// Input: Start key (K), End key (K), Copy function (CopyFunc)
// Output: Slice of pointers to copied values, error if any
func (skipList *SkipList[K, V]) QueryCopies(startKey K, endKey K, copyFunction CopyFunc[K, V]) ([]*V, error) {
	for {
		results := make([]*V, 0)
		resultsCopy := make([]*V, 0)

		predecessor := skipList.head
		level := skipList.maxLevel - 1

		// Traverse down to level 0
		for level >= 0 {
			current := predecessor.nextNodes[level].Load()
			for startKey > current.nodeKey {
				predecessor = current
				current = predecessor.nextNodes[level].Load()
			}
			level--
		}

		// Collect nodes in the range
		current := predecessor.nextNodes[0].Load()
		for current.nodeKey < endKey {
			// Skip marked or not fully linked nodes
			if current.isFullyLinked.Load() && !current.isMarked.Load() {
				value := current.nodeValue.Load()
				if value != nil {
					results = append(results, value)
					valueCopy, err := copyFunction(value)
					if err != nil {
						return nil, err
					}
					resultsCopy = append(resultsCopy, valueCopy)
				}
			}
			current = current.nextNodes[0].Load()
		}
		previousOpCount := skipList.opCount.Load()

		resultsValidation := make([]*V, 0)
		current = predecessor.nextNodes[0].Load()
		for current.nodeKey < endKey {
			// Skip marked or not fully linked nodes
			if current.isFullyLinked.Load() && !current.isMarked.Load() {
				value := current.nodeValue.Load()
				if value != nil {
					resultsValidation = append(resultsValidation, value)
				}
			}
			current = current.nextNodes[0].Load()
		}
		currentOpCount := skipList.opCount.Load()

		if previousOpCount == currentOpCount && reflect.DeepEqual(results, resultsValidation) {
			return resultsCopy, nil
		} else {
			continue
		}
	}
}

// Delete removes the node with the specified key from the SkipList.
// Input: Key (K)
// Output: Boolean indicating if deleted (bool), error if any
func (skipList *SkipList[K, V]) Delete(key K) (bool, error) {
	for {
		levelFound, predecessors, successors := skipList.find(key)
		if levelFound == -1 {
			// Node not found
			return false, nil
		}

		nodeToRemove := successors[levelFound].Load()
		// Lock the node to be removed
		nodeToRemove.mu.Lock()

		// Check if the node is already marked or not fully linked
		if nodeToRemove.isMarked.Load() || !nodeToRemove.isFullyLinked.Load() || nodeToRemove.maxLevel != levelFound {
			nodeToRemove.mu.Unlock()
			return false, nil
		}

		// Lock unique predecessors
		uniquePredecessorsLocked := make(map[*SkipNode[K, V]]bool, len(predecessors))
		valid := true
		for level := 0; valid && level <= nodeToRemove.maxLevel; level++ {
			predecessor := predecessors[level].Load()
			_, locked := uniquePredecessorsLocked[predecessor]
			if !locked {
				predecessor.mu.Lock()
				uniquePredecessorsLocked[predecessor] = true
			}

			unmarked := !predecessors[level].Load().isMarked.Load() && !successors[level].Load().isMarked.Load()
			connected := predecessors[level].Load().nextNodes[level].Load() == successors[level].Load()
			valid = unmarked && connected
		}

		if !valid {
			nodeToRemove.mu.Unlock()
			// Unlock all locked nodes
			for predecessor := range uniquePredecessorsLocked {
				predecessor.mu.Unlock()
			}
			continue
		}

		// Mark the node as logically deleted
		nodeToRemove.isMarked.Store(true)

		// Physically unlink the node from all levels
		for level := nodeToRemove.maxLevel; level >= 0; level-- {
			predecessors[level].Load().nextNodes[level].Store(nodeToRemove.nextNodes[level].Load())
		}

		// Unlock all locked nodes
		for predecessor := range uniquePredecessorsLocked {
			predecessor.mu.Unlock()
		}
		nodeToRemove.mu.Unlock()

		skipList.opCount.Add(1)
		return true, nil
	}
}

// Query retrieves all nodes with keys between startKey and endKey.
// Input: Start key (K), End key (K)
// Output: Slice of pointers to values, error if any
func (skipList *SkipList[K, V]) Query(startKey, endKey K) ([]*V, error) {
	for {
		results := make([]*V, 0)

		predecessor := skipList.head
		level := skipList.maxLevel - 1

		// Traverse down to level 0
		for level >= 0 {
			current := predecessor.nextNodes[level].Load()
			for startKey > current.nodeKey {
				predecessor = current
				current = predecessor.nextNodes[level].Load()
			}
			level--
		}

		// Collect nodes in the range
		current := predecessor.nextNodes[0].Load()
		for current.nodeKey < endKey {
			// Skip marked or not fully linked nodes
			if current.isFullyLinked.Load() && !current.isMarked.Load() {
				value := current.nodeValue.Load()
				if value != nil {
					results = append(results, value)
				}
			}
			current = current.nextNodes[0].Load()
		}
		previousOpCount := skipList.opCount.Load()

		resultsValidation := make([]*V, 0)
		current = predecessor.nextNodes[0].Load()
		for current.nodeKey < endKey {
			// Skip marked or not fully linked nodes
			if current.isFullyLinked.Load() && !current.isMarked.Load() {
				value := current.nodeValue.Load()
				if value != nil {
					resultsValidation = append(resultsValidation, value)
				}
			}
			current = current.nextNodes[0].Load()
		}
		currentOpCount := skipList.opCount.Load()

		if previousOpCount == currentOpCount && reflect.DeepEqual(results, resultsValidation) {
			return results, nil
		} else {
			continue
		}
	}
}

// find locates the node with the given key and populates the path for insertion or deletion.
// Input: Key (K)
// Output: Level found (int), slice of predecessor pointers, slice of successor pointers
func (skipList *SkipList[K, V]) find(key K) (int, []atomic.Pointer[SkipNode[K, V]], []atomic.Pointer[SkipNode[K, V]]) {
	predecessors := make([]atomic.Pointer[SkipNode[K, V]], skipList.maxLevel+1)
	successors := make([]atomic.Pointer[SkipNode[K, V]], skipList.maxLevel+1)

	foundLevel := -1
	predecessor := skipList.head

	level := skipList.maxLevel

	for level >= 0 {
		current := predecessor.nextNodes[level].Load()

		for key > current.nodeKey {
			predecessor = current
			current = predecessor.nextNodes[level].Load()
		}
		if foundLevel == -1 && key == current.nodeKey {
			foundLevel = level
		}
		predecessors[level].Store(predecessor)
		successors[level].Store(current)
		level--
	}
	return foundLevel, predecessors, successors
}

// Find locates a node with the given key.
// Input: Key (K)
// Output: Pointer to value (*V), boolean indicating if found (bool)
func (skipList *SkipList[K, V]) Find(key K) (*V, bool) {
	foundLevel, _, successors := skipList.find(key)

	if foundLevel == -1 {
		return nil, false
	}
	foundNode := successors[foundLevel].Load()
	return foundNode.nodeValue.Load(), foundNode.isFullyLinked.Load() && !foundNode.isMarked.Load() && foundNode.maxLevel == foundLevel
}

// Visualize prints the entire SkipList structure.
// Input: None
// Output: None
func (skipList *SkipList[K, V]) Visualize() {
	fmt.Println("SkipList Visualization:")
	for level := skipList.maxLevel - 1; level >= 0; level-- {
		current := skipList.head
		fmt.Printf("Level %d: ", level)
		for current != nil {
			if current.nodeKey != skipList.head.nodeKey && current.nodeKey != skipList.tail.nodeKey {
				fmt.Printf("|%v| -> ", current.nodeKey)
			} else {
				fmt.Printf("HEAD -> ")
			}
			current = current.nextNodes[level].Load()
		}
		fmt.Println("TAIL")
	}
}
