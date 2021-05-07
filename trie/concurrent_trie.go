package trie

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

// AsyncTrie has additional methods and data fields over
// Trie that allow for futures of data to be returned
// and allows for async IO within a Trie.

type AsyncTrie struct {
	trie Trie
	db   *Database

	resolvedCache map[common.Hash]node
	cacheLock     *sync.RWMutex

	resolveInProgress map[common.Hash]struct{}
	inProgressLock    *sync.RWMutex

	jobs chan prefetchJobWrapper

	// we keep track of inserts so that
	// we know when prefetch fails are legitimate
	updateDirties map[string]struct{}
}

type getReturnType struct {
	value []byte
	err   error
}

type trieGetJob struct {
	key []byte
	ret chan getReturnType
}

type trieDeleteJob struct {
	key []byte
	ret chan error
}

type trieInsertJob struct {
	key   []byte
	value []byte
	ret   chan error
}

type trieKillJob struct{}

type asyncTrieJob interface {
	isTrieJob()
}

func (j trieGetJob) isTrieJob()    {}
func (j trieInsertJob) isTrieJob() {}
func (j trieDeleteJob) isTrieJob() {}
func (j trieKillJob) isTrieJob()   {}

type prefetchJobWrapper struct {
	job             asyncTrieJob
	prefetchSuccess chan error
}

// NewAsync intializes an AsyncTrie
func NewAsync(root common.Hash, db *Database) (*AsyncTrie, error) {
	if db == nil {
		panic("trie.New called without a database")
	}
	trie := &Trie{
		db: db,
	}
	if root != (common.Hash{}) && root != emptyRoot {
		rootnode, err := trie.resolveHash(root[:], nil)
		if err != nil {
			return nil, err
		}
		trie.root = rootnode
	}

	at := AsyncTrie{
		trie:          *trie,
		db:            db,
		resolvedCache: make(map[common.Hash]node),
		jobs:          make(chan prefetchJobWrapper),
		updateDirties: make(map[string]struct{}),
	}

	go at.loop()
	return &at, nil
}

func (t *AsyncTrie) loop() {
	for prefetchJob := range t.jobs {
		// We wait until the prefetch thread returns
		// This ensures sequentiality
		err := <-prefetchJob.prefetchSuccess

		if err != nil {
			switch j := prefetchJob.job.(type) {
			case trieGetJob:
				// If node has not been updated, then the get
				// operation was an error
				if _, ok := t.updateDirties[string(j.key)]; !ok {
					j.ret <- getReturnType{nil, err}
				}

			case trieDeleteJob:
				// If node has not been updated, then the delete
				// operation was an error
				if _, ok := t.updateDirties[string(j.key)]; !ok {
					j.ret <- err
				}
			// we don't support any other type of jobs for now
			default:
			}
		}

		// If prefetch was a success
		switch j := prefetchJob.job.(type) {
		case trieGetJob:
			// This op should return immediately
			node, err := t.trie.TryGet(j.key)
			j.ret <- getReturnType{node, err}
		case trieDeleteJob:
			// This op should return immediately
			err := t.trie.TryDelete(j.key)
			// if success, mark as dirty
			if err == nil {
				t.updateDirties[string(j.key)] = struct{}{}
			}
			j.ret <- err
		// we don't support any other type of jobs for now
		default:
		}

	}
}

// Go worker that fetches the data from disk, cacheing nodes
// in the `resolvedCache`. It walks the current (outdated) `Trie` with assistance
// of the `resolvedCache`. The walking is stable under `Trie` mutation,
// since if the node required is not in `Trie` then it hasn't been mutated,
// which means it can be fetched freely from disk.

// Of course, we get inefficiencies where an intermediate path is deleted
// before a prefetcher

// if prefetch_success is false, while the node is not dirty, the job will
// cancel with a PrefetchFailed error returned to the future.
func (t *AsyncTrie) prefetchAsyncIO(prefetchSuccess chan error, origNode node, key []byte) {
	prefetchSuccess <- t.prefetchConcurrent(origNode, key, 0)
}

func (t *AsyncTrie) GetAsync(origNode node, key []byte) chan getReturnType {
	prefetchSuccess := make(chan error)
	go t.prefetchAsyncIO(prefetchSuccess, origNode, key)

	ret := make(chan getReturnType)

	t.jobs <- prefetchJobWrapper{
		job:             trieGetJob{key: key, ret: ret},
		prefetchSuccess: prefetchSuccess,
	}

	return ret
}

func (t *AsyncTrie) tryGet(origNode node, key []byte, pos int) (value []byte, newnode node, didResolve bool, err error) {
	switch n := (origNode).(type) {
	case nil:
		return nil, nil, false, nil
	case valueNode:
		return n, n, false, nil
	case *shortNode:
		if len(key)-pos < len(n.Key) || !bytes.Equal(n.Key, key[pos:pos+len(n.Key)]) {
			// key not found in trie
			return nil, n, false, nil
		}
		value, newnode, didResolve, err = t.tryGet(n.Val, key, pos+len(n.Key))
		if err == nil && didResolve {
			n = n.copy()
			n.Val = newnode
		}
		return value, n, didResolve, err
	case *fullNode:
		value, newnode, didResolve, err = t.tryGet(n.Children[key[pos]], key, pos+1)
		if err == nil && didResolve {
			n = n.copy()
			n.Children[key[pos]] = newnode
		}
		return value, n, didResolve, err
	case hashNode:
		child, err := t.cachedResolveHash(n, key[:pos])
		if err != nil {
			return nil, n, true, err
		}
		value, newnode, _, err := t.tryGet(child, key, pos)
		return value, newnode, true, err
	default:
		panic(fmt.Sprintf("%T: invalid node: %v", origNode, origNode))
	}
}

func (t *AsyncTrie) prefetchConcurrent(origNode node, key []byte, pos int) error {
	switch n := (origNode).(type) {
	case nil:
		return nil
	case valueNode:
		return nil
	case *shortNode:
		if len(key)-pos < len(n.Key) || !bytes.Equal(n.Key, key[pos:pos+len(n.Key)]) {
			// key not found in trie
			return nil
		}
		return t.prefetchConcurrent(n.Val, key, pos+len(n.Key))
	case *fullNode:
		return t.prefetchConcurrent(n.Children[key[pos]], key, pos+1)
	case hashNode:
		child, err := t.cachedResolveHash(n, key[:pos])
		if err != nil {
			return err
		}
		return t.prefetchConcurrent(child, key, pos)
	default:
		panic(fmt.Sprintf("%T: invalid node: %v", origNode, origNode))
	}
}

func (t *AsyncTrie) cachedResolveHash(h hashNode, prefix []byte) (node, error) {
	hash := common.BytesToHash(h)

	for {
		t.inProgressLock.RLock()
		// if the hash is not being resolved, continue
		if _, ok := t.resolveInProgress[hash]; !ok {
			t.inProgressLock.RUnlock()
			break
		}
		t.inProgressLock.RUnlock()
	}

	t.cacheLock.RLock()
	if node, ok := t.resolvedCache[hash]; ok {
		t.cacheLock.RUnlock()
		return node, nil
	}
	t.cacheLock.RUnlock()

	// If we can't find the hash in the cache, we
	// need to resolve by reading from trie.Database

	// We have to write lock while resolving
	t.inProgressLock.Lock()
	t.resolveInProgress[hash] = struct{}{}
	t.inProgressLock.Unlock()

	node, err := t.trie.resolveHash(h, prefix)

	if err != nil {
		t.cacheLock.Lock()
		t.resolvedCache[hash] = node
		t.cacheLock.Unlock()
	}

	t.inProgressLock.Lock()
	delete(t.resolveInProgress, hash)
	t.inProgressLock.Unlock()

	return node, err
}

// Get returns the value for key stored in the trie.
// The value bytes must not be modified by the caller.
func (t *AsyncTrie) Get(key []byte) []byte {
	res, err := t.TryGet(key)
	if err != nil {
		log.Error(fmt.Sprintf("Unhandled trie error: %v", err))
	}
	return res
}

// TryGet returns the value for key stored in the trie.
// The value bytes must not be modified by the caller.
// If a node was not found in the database, a MissingNodeError is returned.
func (t *AsyncTrie) TryGet(key []byte) ([]byte, error) {
	return t.trie.TryGet(key)
}

// TryGetNode attempts to retrieve a trie node by compact-encoded path. It is not
// possible to use keybyte-encoding as the path might contain odd nibbles.
func (t *AsyncTrie) TryGetNode(path []byte) ([]byte, int, error) {
	return t.trie.TryGetNode(path)
}

// Update associates key with value in the trie. Subsequent calls to
// Get will return value. If value has length zero, any existing value
// is deleted from the trie and calls to Get will return nil.
//
// The value bytes must not be modified by the caller while they are
// stored in the trie.
func (t *AsyncTrie) Update(key, value []byte) {
	if err := t.TryUpdate(key, value); err != nil {
		log.Error(fmt.Sprintf("Unhandled trie error: %v", err))
	}
}

// TryUpdate associates key with value in the trie. Subsequent calls to
// Get will return value. If value has length zero, any existing value
// is deleted from the trie and calls to Get will return nil.
//
// The value bytes must not be modified by the caller while they are
// stored in the trie.
//
// If a node was not found in the database, a MissingNodeError is returned.
func (t *AsyncTrie) TryUpdate(key, value []byte) error {
	return t.trie.TryUpdate(key, value)
}

// Delete removes any existing value for key from the trie.
func (t *AsyncTrie) Delete(key []byte) {
	if err := t.TryDelete(key); err != nil {
		log.Error(fmt.Sprintf("Unhandled trie error: %v", err))
	}
}

// TryDelete removes any existing value for key from the trie.
// If a node was not found in the database, a MissingNodeError is returned.
func (t *AsyncTrie) TryDelete(key []byte) error {
	return t.trie.TryDelete(key)
}

// Commit writes all nodes and the secure hash pre-images to the trie's database.
// Nodes are stored with their sha3 hash as the key.
//
// Committing flushes nodes from memory. Subsequent Get calls will load nodes
// from the database.
func (t *AsyncTrie) Commit(onleaf LeafCallback) (root common.Hash, err error) {
	// Commit the trie to its intermediate node database
	return t.trie.Commit(onleaf)
}

// Hash returns the root hash of AsyncTrie. It does not write to the
// database and can be used even if the trie doesn't have one.
func (t *AsyncTrie) Hash() common.Hash {
	return t.trie.Hash()
}

// Copy returns a copy of AsyncTrie.
func (t *AsyncTrie) Copy() *AsyncTrie {
	cpy := *t
	return &cpy
}

// NodeIterator returns an iterator that returns nodes of the underlying trie. Iteration
// starts at the key after the given start key.
func (t *AsyncTrie) NodeIterator(start []byte) NodeIterator {
	return t.trie.NodeIterator(start)
}
