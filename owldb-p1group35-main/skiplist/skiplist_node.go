package skiplist

import (
	"cmp"
	"sync"
	"sync/atomic"
)

// SkipNode is the structure that holds the key-value pairs in the SkipList.
type SkipNode[K cmp.Ordered, V any] struct {
	mu            sync.Mutex
	nodeKey       K
	nodeValue     atomic.Pointer[V]
	maxLevel      int
	isMarked      atomic.Bool
	isFullyLinked atomic.Bool
	nextNodes     []atomic.Pointer[SkipNode[K, V]]
}

// InitializeNode creates a new SkipNode with the given key, value, and level.
func InitializeNode[K cmp.Ordered, V any](key K, value *V, level int) *SkipNode[K, V] {
	newNode := &SkipNode[K, V]{
		nodeKey:   key,
		maxLevel:  level,
		nextNodes: make([]atomic.Pointer[SkipNode[K, V]], level+1),
	}
	if value != nil {
		newNode.nodeValue.Store(value)
	}
	return newNode
}
