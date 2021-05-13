package trie

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
)

// AsyncTrie has additional methods and data fields over
// Trie that allow for futures of data to be returned
// and allows for async IO within a Trie.

/// GetReturnType is an exported type
type GetReturnType struct {
	value []byte
	err   error
}
type triePrefetchJob struct {
	ret chan error
}
type trieGetJob struct {
	key []byte
	ret chan GetReturnType
}

type trieDeleteJob struct {
	key []byte
	ret chan error
}

type trieUpdateJob struct {
	key   []byte
	value []byte
	ret   chan error
}

type trieKillJob struct {
	synced chan struct{}
}

type asyncTrieJob interface {
	isTrieJob()
}

func (j triePrefetchJob) isTrieJob() {}
func (j trieGetJob) isTrieJob()      {}
func (j trieUpdateJob) isTrieJob()   {}
func (j trieDeleteJob) isTrieJob()   {}
func (j trieKillJob) isTrieJob()     {}

type prefetchJobWrapper struct {
	job             asyncTrieJob
	prefetchSuccess chan error
}

type AsyncTrie struct {
	db   *Database
	root node
	// Keep track of the number leafs which have been inserted since the last
	// hashing operation. This number will not directly map to the number of
	// actually unhashed nodes
	unhashed int

	resolvedCache ConcurrentMap

	jobs chan prefetchJobWrapper

	rootLock *sync.RWMutex

	// we keep track of inserts so that
	// we know when prefetch fails are legitimate
	updateDirties map[string]struct{}

	jobNumber int
}

// NewAsync intializes an AsyncTrie
func NewAsync(root common.Hash, db *Database) (*AsyncTrie, error) {
	if db == nil {
		panic("trie.NewAsync called without a database")
	}

	at := AsyncTrie{
		db: db,

		resolvedCache: NewCMap(),

		jobs: make(chan prefetchJobWrapper, 256),

		updateDirties: make(map[string]struct{}),

		rootLock: &sync.RWMutex{},

		jobNumber: 0,
	}

	if root != (common.Hash{}) && root != emptyRoot {
		rootnode, err := at.resolveHash(root[:], nil)
		if err != nil {
			return nil, err
		}
		at.setRoot(rootnode)
	}

	go at.loop()
	return &at, nil
}

func (t *AsyncTrie) sync(clearCache bool) {
	prefetchSuccess := make(chan error, 1)
	synced := make(chan struct{})
	t.jobs <- prefetchJobWrapper{
		trieKillJob{
			synced: synced,
		},
		prefetchSuccess,
	}
	prefetchSuccess <- nil

	<-synced
	if clearCache {
		t.resolvedCache.Clear()
	}

	synced <- struct{}{}
}

func (t *AsyncTrie) loop() {
	for prefetchJob := range t.jobs {
		err := <-prefetchJob.prefetchSuccess

		close(prefetchJob.prefetchSuccess)
		t.jobNumber++

		if err != nil {
			switch j := prefetchJob.job.(type) {
			case triePrefetchJob:
				j.ret <- err
			case trieGetJob:
				// If node has not been updated, then the get
				// operation was an error
				if _, ok := t.updateDirties[string(j.key)]; !ok {
					j.ret <- GetReturnType{nil, err}
				}
				continue

			case trieDeleteJob:
				// If node has not been updated, then the delete
				// operation was an error
				if _, ok := t.updateDirties[string(j.key)]; !ok {
					j.ret <- err
				}
				continue
			default:
			}
		}

		// If prefetch was a success
		switch j := prefetchJob.job.(type) {

		case trieKillJob:
			j.synced <- struct{}{}
			// fmt.Println(fmt.Sprintf("WAITING TO SYNC @ job %v", t.jobNumber))
			<-j.synced
		case triePrefetchJob:
			j.ret <- nil
		case trieGetJob:
			// This op should return immediately
			val, err := t.TryGet(j.key)
			j.ret <- GetReturnType{val, err}
		case trieDeleteJob:
			// This op should return immediately
			err := t.TryDelete(j.key)
			// if success, mark as dirty
			if err == nil {
				t.updateDirties[string(j.key)] = struct{}{}
			}
			j.ret <- err
		case trieUpdateJob:
			err := t.TryUpdate(j.key, j.value)
			if err == nil {
				t.updateDirties[string(j.key)] = struct{}{}
			}
			j.ret <- nil
		// we don't support any other type of jobs for now
		default:
			panic(fmt.Sprintf("Invalid Job"))
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
	prefetchSuccess <- t.prefetchConcurrent(origNode, keybytesToHex(key), 0)
}

func (t *AsyncTrie) TryGetAsync(key []byte) chan GetReturnType {
	prefetchSuccess := make(chan error, 1)

	go t.prefetchAsyncIO(prefetchSuccess, t.getRoot(), copyKey(key))

	ret := make(chan GetReturnType, 1)

	t.jobs <- prefetchJobWrapper{
		job:             trieGetJob{key: copyKey(key), ret: ret},
		prefetchSuccess: prefetchSuccess,
	}
	// fmt.Println("Creating job for", t.jobNumber, key2)

	return ret
}

func copyKey(key []byte) []byte {
	tmp := make([]byte, len(key))
	copy(tmp, key)
	return tmp
}

func (t *AsyncTrie) TryUpdateAsync(key []byte, value []byte) chan error {
	prefetchSuccess := make(chan error, 1)
	go t.prefetchAsyncIO(prefetchSuccess, t.getRoot(), copyKey(key))

	ret := make(chan error, 1)

	t.jobs <- prefetchJobWrapper{
		job:             trieUpdateJob{key: copyKey(key), value: value, ret: ret},
		prefetchSuccess: prefetchSuccess,
	}

	return ret
}

func (t *AsyncTrie) PrefetchAsync(key []byte) chan error {
	prefetchSuccess := make(chan error, 1)
	go t.prefetchAsyncIO(prefetchSuccess, t.getRoot(), copyKey(key))

	ret := make(chan error, 1)

	t.jobs <- prefetchJobWrapper{
		job:             triePrefetchJob{ret: ret},
		prefetchSuccess: prefetchSuccess,
	}

	return ret
}

// TryGet returns the value for key stored in the trie.
// The value bytes must not be modified by the caller.
// If a node was not found in the database, a MissingNodeError is returned.
func (t *AsyncTrie) TryGet(key []byte) ([]byte, error) {
	value, newroot, didResolve, err := t.tryGet(t.getRoot(), keybytesToHex(key), 0)
	if err == nil && didResolve {
		t.setRoot(newroot)
	}
	return value, err
}

func (t *AsyncTrie) tryGet(origNode node, key []byte, pos int) (value []byte, newnode node, didResolve bool, err error) {
	// fmt.Println(fmt.Sprintf("(Prefetched 2) Resolving"))

	// fmt.Println(fmt.Sprintf("getting key:      %v", key))
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
		child, err := t.resolveHashPrefetched(n, key[:pos])
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
		child, err := t.resolveHash(n, key[:pos])
		if err != nil {
			return err
		}
		return t.prefetchConcurrent(child, key, pos)
	default:
		panic(fmt.Sprintf("%T: invalid node: %v", origNode, origNode))
	}

}

func (t *AsyncTrie) resolveHash(h hashNode, prefix []byte) (node, error) {
	hash := common.BytesToHash(h)
	if node, ok := t.resolvedCache.Get(hash); ok {
		// fmt.Println(fmt.Sprintf("HASH WAS RESOLVED FROM CACHE: %v", hash))
		return node, nil
	}

	shard := t.resolvedCache.GetShard(hash)
	shard.Lock()
	if node, ok := shard.items[hash]; ok {
		shard.Unlock()
		return node, nil
	}

	// fmt.Println(fmt.Sprintf("HASH IS RESOLVING FROM DISK: %v", hash))
	node, err := t.resolveHashInternal(hash, prefix)
	if err == nil {
		shard.items[hash] = node
	}
	shard.Unlock()

	return node, err
}

func (t *AsyncTrie) resolveHashPrefetched(h hashNode, prefix []byte) (node, error) {
	hash := common.BytesToHash(h)

	if node, ok := t.resolvedCache.Get(hash); ok {

		// fmt.Println(fmt.Sprintf("(Prefetched 0) HASH WAS RESOLVED FROM CACHE: %v", hash))
		return node, nil
	}

	shard := t.resolvedCache.GetShard(hash)
	shard.Lock()
	if node, ok := shard.items[hash]; ok {
		// fmt.Println(fmt.Sprintf("(Prefetched 1) HASH WAS RESOLVED FROM CACHE: %v", hash))
		shard.Unlock()
		return node, nil
	}

	// fmt.Println(fmt.Sprintf("(Prefetch) HASH IS RESOLVING FROM DISK: %v", hash))
	node, err := t.resolveHashInternal(hash, prefix)
	// _, ok := shard.items[hash]
	if err == nil {
		// panic(fmt.Sprintf("GET DISK NOT EXPECTED OR ALLOWED: hash - %v, %v, %v", hash, err, ok))
		shard.items[hash] = node
	}
	shard.Unlock()

	return node, err
}

func (t *AsyncTrie) resolveHashInternal(hash common.Hash, prefix []byte) (node, error) {
	// fmt.Println(fmt.Sprintf("HASH IS RESOLVING: %v", hash))
	if node := t.db.node(hash); node != nil {
		return node, nil
	}
	// panic("DISK RESOLVE FAILED")
	return nil, &MissingNodeError{NodeHash: hash, Path: prefix}
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

// Copy returns a copy of AsyncTrie.
func (t *AsyncTrie) Copy() *AsyncTrie {
	cpy := *t
	return &cpy
}

func (t *AsyncTrie) AsTrie() *Trie {
	return &Trie{
		root:     t.getRoot(),
		db:       t.db,
		unhashed: t.unhashed,
	}
}

func (t *AsyncTrie) getRoot() node {
	// t.rootLock.RLock()
	root := t.root
	// t.rootLock.RUnlock()
	return root
}

func (t *AsyncTrie) setRoot(n node) {
	// t.rootLock.Lock()
	t.root = n
	// t.rootLock.Unlock()
}

// NodeIterator returns an iterator that returns nodes of the trie. Iteration starts at
// the key after the given start key.
func (t *AsyncTrie) NodeIterator(start []byte) NodeIterator {
	return newNodeIterator(t.AsTrie(), start)
}

// TryGetNode attempts to retrieve a trie node by compact-encoded path. It is not
// possible to use keybyte-encoding as the path might contain odd nibbles.
func (t *AsyncTrie) TryGetNode(path []byte) ([]byte, int, error) {
	item, newroot, resolved, err := t.tryGetNode(t.getRoot(), compactToHex(path), 0)
	if err != nil {
		return nil, resolved, err
	}
	if resolved > 0 {
		t.setRoot(newroot)
	}
	if item == nil {
		return nil, resolved, nil
	}
	return item, resolved, err
}

func (t *AsyncTrie) tryGetNode(origNode node, path []byte, pos int) (item []byte, newnode node, resolved int, err error) {
	// If we reached the requested path, return the current node
	if pos >= len(path) {
		// Although we most probably have the original node expanded, encoding
		// that into consensus form can be nasty (needs to cascade down) and
		// time consuming. Instead, just pull the hash up from disk directly.
		var hash hashNode
		if node, ok := origNode.(hashNode); ok {
			hash = node
		} else {
			hash, _ = origNode.cache()
		}
		if hash == nil {
			return nil, origNode, 0, errors.New("non-consensus node")
		}
		blob, err := t.db.Node(common.BytesToHash(hash))
		return blob, origNode, 1, err
	}
	// Path still needs to be traversed, descend into children
	switch n := (origNode).(type) {
	case nil:
		// Non-existent path requested, abort
		return nil, nil, 0, nil

	case valueNode:
		// Path prematurely ended, abort
		return nil, nil, 0, nil

	case *shortNode:
		if len(path)-pos < len(n.Key) || !bytes.Equal(n.Key, path[pos:pos+len(n.Key)]) {
			// Path branches off from short node
			return nil, n, 0, nil
		}
		item, newnode, resolved, err = t.tryGetNode(n.Val, path, pos+len(n.Key))
		if err == nil && resolved > 0 {
			n = n.copy()
			n.Val = newnode
		}
		return item, n, resolved, err

	case *fullNode:
		item, newnode, resolved, err = t.tryGetNode(n.Children[path[pos]], path, pos+1)
		if err == nil && resolved > 0 {
			n = n.copy()
			n.Children[path[pos]] = newnode
		}
		return item, n, resolved, err

	case hashNode:
		child, err := t.resolveHash(n, path[:pos])
		if err != nil {
			return nil, n, 1, err
		}
		item, newnode, resolved, err := t.tryGetNode(child, path, pos)
		return item, newnode, resolved + 1, err

	default:
		panic(fmt.Sprintf("%T: invalid node: %v", origNode, origNode))
	}
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
	t.unhashed++
	k := keybytesToHex(key)
	if len(value) != 0 {
		_, n, err := t.insert(t.getRoot(), nil, k, valueNode(value))
		if err != nil {
			return err
		}
		t.setRoot(n)
	} else {
		_, n, err := t.delete(t.getRoot(), nil, k)
		if err != nil {
			return err
		}
		t.setRoot(n)
	}

	return nil
}

func (t *AsyncTrie) insert(n node, prefix, key []byte, value node) (bool, node, error) {
	if len(key) == 0 {
		if v, ok := n.(valueNode); ok {
			return !bytes.Equal(v, value.(valueNode)), value, nil
		}
		return true, value, nil
	}
	switch n := n.(type) {
	case *shortNode:

		matchlen := prefixLen(key, n.Key)
		// If the whole key matches, keep this short node as is
		// and only update the value.
		if matchlen == len(n.Key) {
			dirty, nn, err := t.insert(n.Val, append(prefix, key[:matchlen]...), key[matchlen:], value)
			if !dirty || err != nil {
				return false, n, err
			}
			return true, &shortNode{n.Key, nn, t.newFlag()}, nil
		}
		// Otherwise branch out at the index where they differ.
		branch := &fullNode{flags: t.newFlag()}
		var err error
		_, branch.Children[n.Key[matchlen]], err = t.insert(nil, append(prefix, n.Key[:matchlen+1]...), n.Key[matchlen+1:], n.Val)
		if err != nil {
			return false, nil, err
		}
		_, branch.Children[key[matchlen]], err = t.insert(nil, append(prefix, key[:matchlen+1]...), key[matchlen+1:], value)
		if err != nil {
			return false, nil, err
		}
		// Replace this shortNode with the branch if it occurs at index 0.
		if matchlen == 0 {
			return true, branch, nil
		}
		// Otherwise, replace it with a short node leading up to the branch.
		return true, &shortNode{key[:matchlen], branch, t.newFlag()}, nil

	case *fullNode:
		dirty, nn, err := t.insert(n.Children[key[0]], append(prefix, key[0]), key[1:], value)
		if !dirty || err != nil {
			return false, n, err
		}
		n = n.copy()
		n.flags = t.newFlag()
		n.Children[key[0]] = nn
		return true, n, nil

	case nil:
		return true, &shortNode{key, value, t.newFlag()}, nil

	case hashNode:
		// We've hit a part of the trie that isn't loaded yet. Load
		// the node and insert into it. This leaves all child nodes on
		// the path to the value in the trie.
		rn, err := t.resolveHash(n, prefix)
		if err != nil {
			return false, nil, err
		}
		dirty, nn, err := t.insert(rn, prefix, key, value)
		if !dirty || err != nil {
			return false, rn, err
		}
		return true, nn, nil

	default:
		panic(fmt.Sprintf("%T: invalid node: %v", n, n))
	}
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
	t.unhashed++
	k := keybytesToHex(key)
	_, n, err := t.delete(t.getRoot(), nil, k)
	if err != nil {
		return err
	}
	t.setRoot(n)
	return nil
}

// delete returns the new root of the trie with key deleted.
// It reduces the trie to minimal form by simplifying
// nodes on the way up after deleting recursively.
func (t *AsyncTrie) delete(n node, prefix, key []byte) (bool, node, error) {
	switch n := n.(type) {
	case *shortNode:
		matchlen := prefixLen(key, n.Key)
		if matchlen < len(n.Key) {
			return false, n, nil // don't replace n on mismatch
		}
		if matchlen == len(key) {
			return true, nil, nil // remove n entirely for whole matches
		}
		// The key is longer than n.Key. Remove the remaining suffix
		// from the subtrie. Child can never be nil here since the
		// subtrie must contain at least two other values with keys
		// longer than n.Key.
		dirty, child, err := t.delete(n.Val, append(prefix, key[:len(n.Key)]...), key[len(n.Key):])
		if !dirty || err != nil {
			return false, n, err
		}
		switch child := child.(type) {
		case *shortNode:
			// Deleting from the subtrie reduced it to another
			// short node. Merge the nodes to avoid creating a
			// shortNode{..., shortNode{...}}. Use concat (which
			// always creates a new slice) instead of append to
			// avoid modifying n.Key since it might be shared with
			// other nodes.
			return true, &shortNode{concat(n.Key, child.Key...), child.Val, t.newFlag()}, nil
		default:
			return true, &shortNode{n.Key, child, t.newFlag()}, nil
		}

	case *fullNode:
		dirty, nn, err := t.delete(n.Children[key[0]], append(prefix, key[0]), key[1:])
		if !dirty || err != nil {
			return false, n, err
		}
		n = n.copy()
		n.flags = t.newFlag()
		n.Children[key[0]] = nn

		// Check how many non-nil entries are left after deleting and
		// reduce the full node to a short node if only one entry is
		// left. Since n must've contained at least two children
		// before deletion (otherwise it would not be a full node) n
		// can never be reduced to nil.
		//
		// When the loop is done, pos contains the index of the single
		// value that is left in n or -2 if n contains at least two
		// values.
		pos := -1
		for i, cld := range &n.Children {
			if cld != nil {
				if pos == -1 {
					pos = i
				} else {
					pos = -2
					break
				}
			}
		}
		if pos >= 0 {
			if pos != 16 {
				// If the remaining entry is a short node, it replaces
				// n and its key gets the missing nibble tacked to the
				// front. This avoids creating an invalid
				// shortNode{..., shortNode{...}}.  Since the entry
				// might not be loaded yet, resolve it just for this
				// check.
				cnode, err := t.resolve(n.Children[pos], prefix)
				if err != nil {
					return false, nil, err
				}
				if cnode, ok := cnode.(*shortNode); ok {
					k := append([]byte{byte(pos)}, cnode.Key...)
					return true, &shortNode{k, cnode.Val, t.newFlag()}, nil
				}
			}
			// Otherwise, n is replaced by a one-nibble short node
			// containing the child.
			return true, &shortNode{[]byte{byte(pos)}, n.Children[pos], t.newFlag()}, nil
		}
		// n still contains at least two values and cannot be reduced.
		return true, n, nil

	case valueNode:
		return true, nil, nil

	case nil:
		return false, nil, nil

	case hashNode:
		// We've hit a part of the trie that isn't loaded yet. Load
		// the node and delete from it. This leaves all child nodes on
		// the path to the value in the trie.
		rn, err := t.resolveHash(n, prefix)
		if err != nil {
			return false, nil, err
		}
		dirty, nn, err := t.delete(rn, prefix, key)
		if !dirty || err != nil {
			return false, rn, err
		}
		return true, nn, nil

	default:
		panic(fmt.Sprintf("%T: invalid node: %v (%v)", n, n, key))
	}
}

func (t *AsyncTrie) resolve(n node, prefix []byte) (node, error) {
	if n, ok := n.(hashNode); ok {
		return t.resolveHash(n, prefix)
	}
	return n, nil
}

// Hash returns the root hash of the trie. It does not write to the
// database and can be used even if the trie doesn't have one.
func (t *AsyncTrie) Hash() common.Hash {
	hash, cached, _ := t.hashRoot()
	t.setRoot(cached)
	return common.BytesToHash(hash.(hashNode))
}

// Commit writes all nodes to the trie's memory database, tracking the internal
// and external (for account tries) references.
func (t *AsyncTrie) Commit(onleaf LeafCallback) (root common.Hash, err error) {
	if t.db == nil {
		panic("commit called on trie with nil database")
	}
	t.sync(true)
	if t.getRoot() == nil {
		return emptyRoot, nil
	}
	// Derive the hash for all dirty nodes first. We hold the assumption
	// in the following procedure that all nodes are hashed.
	rootHash := t.Hash()
	h := newCommitter()
	defer returnCommitterToPool(h)

	// Do a quick check if we really need to commit, before we spin
	// up goroutines. This can happen e.g. if we load a trie for reading storage
	// values, but don't write to it.
	if _, dirty := t.getRoot().cache(); !dirty {
		return rootHash, nil
	}
	var wg sync.WaitGroup
	if onleaf != nil {
		h.onleaf = onleaf
		h.leafCh = make(chan *leaf, leafChanSize)
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.commitLoop(t.db)
		}()
	}
	var newRoot hashNode
	newRoot, err = h.Commit(t.getRoot(), t.db)
	if onleaf != nil {
		// The leafch is created in newCommitter if there was an onleaf callback
		// provided. The commitLoop only _reads_ from it, and the commit
		// operation was the sole writer. Therefore, it's safe to close this
		// channel here.
		close(h.leafCh)
		wg.Wait()
	}
	if err != nil {
		return common.Hash{}, err
	}
	t.setRoot(newRoot)
	return rootHash, nil
}

// hashRoot calculates the root hash of the given trie
func (t *AsyncTrie) hashRoot() (node, node, error) {
	if t.getRoot() == nil {
		return hashNode(emptyRoot.Bytes()), nil, nil
	}
	// If the number of changes is below 100, we let one thread handle it
	h := newHasher(t.unhashed >= 100)
	defer returnHasherToPool(h)
	hashed, cached := h.hash(t.getRoot(), true)
	t.unhashed = 0
	return hashed, cached, nil
}

// Reset drops the referenced root node and cleans all internal state.
func (t *AsyncTrie) Reset() {
	t.setRoot(nil)
	t.unhashed = 0
}

// newFlag returns the cache flag value for a newly created node.
func (t *AsyncTrie) newFlag() nodeFlag {
	return nodeFlag{dirty: true}
}

// Prove constructs a merkle proof for key. The result contains all encoded nodes
// on the path to the value at key. The value itself is also included in the last
// node and can be retrieved by verifying the proof.
//
// If the trie does not contain a value for key, the returned proof contains all
// nodes of the longest existing prefix of the key (at least the root node), ending
// with the node that proves the absence of the key.
func (t *AsyncTrie) Prove(key []byte, fromLevel uint, proofDb ethdb.KeyValueWriter) error {
	// Collect all nodes on the path to key.
	key = keybytesToHex(key)
	var nodes []node
	tn := t.getRoot()
	for len(key) > 0 && tn != nil {
		switch n := tn.(type) {
		case *shortNode:
			if len(key) < len(n.Key) || !bytes.Equal(n.Key, key[:len(n.Key)]) {
				// The trie doesn't contain the key.
				tn = nil
			} else {
				tn = n.Val
				key = key[len(n.Key):]
			}
			nodes = append(nodes, n)
		case *fullNode:
			tn = n.Children[key[0]]
			key = key[1:]
			nodes = append(nodes, n)
		case hashNode:
			var err error
			tn, err = t.resolveHash(n, nil)
			if err != nil {
				log.Error(fmt.Sprintf("Unhandled trie error: %v", err))
				return err
			}
		default:
			panic(fmt.Sprintf("%T: invalid node: %v", tn, tn))
		}
	}
	hasher := newHasher(false)
	defer returnHasherToPool(hasher)

	for i, n := range nodes {
		if fromLevel > 0 {
			fromLevel--
			continue
		}
		var hn node
		n, hn = hasher.proofHash(n)
		if hash, ok := hn.(hashNode); ok || i == 0 {
			// If the node's database encoding is a hash (or is the
			// root node), it becomes a proof element.
			enc, _ := rlp.EncodeToBytes(n)
			if !ok {
				hash = hasher.hashData(enc)
			}
			proofDb.Put(hash, enc)
		}
	}
	return nil
}
