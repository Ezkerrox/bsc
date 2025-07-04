// Copyright 2022 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package pathdb

import (
	"errors"
	"fmt"
	"sync"

	"github.com/Ezkerrox/bsc/common"
	"github.com/Ezkerrox/bsc/core/rawdb"
	"github.com/Ezkerrox/bsc/crypto"
	"github.com/Ezkerrox/bsc/ethdb"
	"github.com/Ezkerrox/bsc/log"
	"github.com/Ezkerrox/bsc/trie/trienode"
	"github.com/VictoriaMetrics/fastcache"
)

// trienodebuffer is a collection of modified trie nodes to aggregate the disk
// write. The content of the trienodebuffer must be checked before diving into
// disk (since it basically is not-yet-written data).
type trienodebuffer interface {
	// account retrieves the account blob with account address hash.
	account(hash common.Hash) ([]byte, bool)

	// storage retrieves the storage slot with account address hash and slot key.
	storage(addrHash common.Hash, storageHash common.Hash) ([]byte, bool)

	// node retrieves the trie node with given node info.
	node(owner common.Hash, path []byte) (*trienode.Node, bool)

	// commit merges the provided states and trie nodes into the buffer. This operation won't take
	// the ownership of the nodes map which belongs to the bottom-most diff layer.
	// It will just hold the node references from the given map which are safe to
	// copy.
	commit(nodes *nodeSet, states *stateSet) trienodebuffer

	// revertTo is the reverse operation of commit. It also merges the provided states
	// and trie nodes into the buffer. The key difference is that the provided state
	// set should reverse the changes made by the most recent state transition.
	revertTo(db ethdb.KeyValueReader, nodes map[common.Hash]map[string]*trienode.Node, accounts map[common.Hash][]byte, storages map[common.Hash]map[common.Hash][]byte) error

	// flush persists the in-memory dirty trie node into the disk if the configured
	// memory threshold is reached. Note, all data must be written atomically.
	flush(db ethdb.KeyValueStore, freezer ethdb.AncientWriter, clean *fastcache.Cache, id uint64, force bool) error

	// empty returns an indicator if trienodebuffer contains any state transition inside.
	empty() bool

	// waitAndStopFlushing will block unit writing the trie nodes of trienodebuffer to disk.
	waitAndStopFlushing()

	// getAllNodesAndStates return the trie nodes and states cached in nodebuffer.
	getAllNodesAndStates() (*nodeSet, *stateSet)

	// getStates return the states cached in nodebuffer.
	getStates() *stateSet

	// getLayers return the size of cached difflayers.
	getLayers() uint64

	// getSize return the trienodebuffer used size.
	getSize() (uint64, uint64)
}

func NewTrieNodeBuffer(sync bool, limit int, nodes *nodeSet, states *stateSet, layers uint64) trienodebuffer {
	if sync {
		log.Info("New sync node buffer", "limit", common.StorageSize(limit), "layers", layers)
		return newBuffer(limit, nodes, states, layers)
	}
	log.Info("New async node buffer", "limit", common.StorageSize(limit), "layers", layers)
	return newAsyncNodeBuffer(limit, nodes, states, layers)
}

// diskLayer is a low level persistent layer built on top of a key-value store.
type diskLayer struct {
	root   common.Hash      // Immutable, root hash to which this layer was made for
	id     uint64           // Immutable, corresponding state id
	db     *Database        // Path-based trie database
	nodes  *fastcache.Cache // GC friendly memory cache of clean nodes
	buffer trienodebuffer   // Dirty buffer to aggregate writes of nodes and states
	stale  bool             // Signals that the layer became stale (state progressed)
	lock   sync.RWMutex     // Lock used to protect stale flag
}

// newDiskLayer creates a new disk layer based on the passing arguments.
func newDiskLayer(root common.Hash, id uint64, db *Database, nodes *fastcache.Cache, buffer trienodebuffer) *diskLayer {
	// Initialize a clean cache if the memory allowance is not zero
	// or reuse the provided cache if it is not nil (inherited from
	// the original disk layer).
	if nodes == nil && db.config.CleanCacheSize != 0 {
		nodes = fastcache.New(db.config.CleanCacheSize)
	}
	return &diskLayer{
		root:   root,
		id:     id,
		db:     db,
		nodes:  nodes,
		buffer: buffer,
	}
}

// rootHash implements the layer interface, returning root hash of corresponding state.
func (dl *diskLayer) rootHash() common.Hash {
	return dl.root
}

// stateID implements the layer interface, returning the state id of disk layer.
func (dl *diskLayer) stateID() uint64 {
	return dl.id
}

// parentLayer implements the layer interface, returning nil as there's no layer
// below the disk.
func (dl *diskLayer) parentLayer() layer {
	return nil
}

// isStale return whether this layer has become stale (was flattened across) or if
// it's still live.
func (dl *diskLayer) isStale() bool {
	dl.lock.RLock()
	defer dl.lock.RUnlock()

	return dl.stale
}

// markStale sets the stale flag as true.
func (dl *diskLayer) markStale() {
	dl.lock.Lock()
	defer dl.lock.Unlock()

	if dl.stale {
		panic("triedb disk layer is stale") // we've committed into the same base from two children, boom
	}
	dl.stale = true
}

// node implements the layer interface, retrieving the trie node with the
// provided node info. No error will be returned if the node is not found.
func (dl *diskLayer) node(owner common.Hash, path []byte, hash common.Hash, depth int) ([]byte, common.Hash, *nodeLoc, error) {
	dl.lock.RLock()
	defer dl.lock.RUnlock()

	if dl.stale {
		return nil, common.Hash{}, nil, errSnapshotStale
	}
	// Try to retrieve the trie node from the not-yet-written
	// node buffer first. Note the buffer is lock free since
	// it's impossible to mutate the buffer before tagging the
	// layer as stale.
	n, found := dl.buffer.node(owner, path)
	if found {
		dirtyNodeHitMeter.Mark(1)
		dirtyNodeReadMeter.Mark(int64(len(n.Blob)))
		dirtyNodeHitDepthHist.Update(int64(depth))
		return n.Blob, n.Hash, &nodeLoc{loc: locDirtyCache, depth: depth}, nil
	}
	dirtyNodeMissMeter.Mark(1)

	// Try to retrieve the trie node from the clean memory cache
	h := newHasher()
	defer h.release()

	key := nodeCacheKey(owner, path)
	if dl.nodes != nil {
		if blob := dl.nodes.Get(nil, key); len(blob) > 0 {
			cleanNodeHitMeter.Mark(1)
			cleanNodeReadMeter.Mark(int64(len(blob)))
			return blob, h.hash(blob), &nodeLoc{loc: locCleanCache, depth: depth}, nil
		}
		cleanNodeMissMeter.Mark(1)
	}
	// Try to retrieve the trie node from the disk.
	var blob []byte
	if owner == (common.Hash{}) {
		blob = rawdb.ReadAccountTrieNode(dl.db.diskdb, path)
	} else {
		blob = rawdb.ReadStorageTrieNode(dl.db.diskdb, owner, path)
	}
	if dl.nodes != nil && len(blob) > 0 {
		dl.nodes.Set(key, blob)
		cleanNodeWriteMeter.Mark(int64(len(blob)))
	}
	return blob, h.hash(blob), &nodeLoc{loc: locDiskLayer, depth: depth}, nil
}

// account directly retrieves the account RLP associated with a particular
// hash in the slim data format.
//
// Note the returned account is not a copy, please don't modify it.
func (dl *diskLayer) account(hash common.Hash, depth int) ([]byte, error) {
	dl.lock.RLock()
	defer dl.lock.RUnlock()

	if dl.stale {
		return nil, errSnapshotStale
	}
	// Try to retrieve the account from the not-yet-written
	// node buffer first. Note the buffer is lock free since
	// it's impossible to mutate the buffer before tagging the
	// layer as stale.
	blob, found := dl.buffer.account(hash)
	if found {
		dirtyStateHitMeter.Mark(1)
		dirtyStateReadMeter.Mark(int64(len(blob)))
		dirtyStateHitDepthHist.Update(int64(depth))

		if len(blob) == 0 {
			stateAccountInexMeter.Mark(1)
		} else {
			stateAccountExistMeter.Mark(1)
		}
		return blob, nil
	}
	dirtyStateMissMeter.Mark(1)

	// TODO(rjl493456442) support persistent state retrieval
	return nil, errors.New("not supported")
}

// storage directly retrieves the storage data associated with a particular hash,
// within a particular account.
//
// Note the returned account is not a copy, please don't modify it.
func (dl *diskLayer) storage(accountHash, storageHash common.Hash, depth int) ([]byte, error) {
	// Hold the lock, ensure the parent won't be changed during the
	// state accessing.
	dl.lock.RLock()
	defer dl.lock.RUnlock()

	if dl.stale {
		return nil, errSnapshotStale
	}
	// Try to retrieve the storage slot from the not-yet-written
	// node buffer first. Note the buffer is lock free since
	// it's impossible to mutate the buffer before tagging the
	// layer as stale.
	if blob, found := dl.buffer.storage(accountHash, storageHash); found {
		dirtyStateHitMeter.Mark(1)
		dirtyStateReadMeter.Mark(int64(len(blob)))
		dirtyStateHitDepthHist.Update(int64(depth))

		if len(blob) == 0 {
			stateStorageInexMeter.Mark(1)
		} else {
			stateStorageExistMeter.Mark(1)
		}
		return blob, nil
	}
	dirtyStateMissMeter.Mark(1)

	// TODO(rjl493456442) support persistent state retrieval
	return nil, errors.New("not supported")
}

// update implements the layer interface, returning a new diff layer on top
// with the given state set.
func (dl *diskLayer) update(root common.Hash, id uint64, block uint64, nodes *nodeSet, states *StateSetWithOrigin) *diffLayer {
	return newDiffLayer(dl, root, id, block, nodes, states)
}

// commit merges the given bottom-most diff layer into the node buffer
// and returns a newly constructed disk layer. Note the current disk
// layer must be tagged as stale first to prevent re-access.
func (dl *diskLayer) commit(bottom *diffLayer, force bool) (*diskLayer, error) {
	dl.lock.Lock()
	defer dl.lock.Unlock()

	// Construct and store the state history first. If crash happens after storing
	// the state history but without flushing the corresponding states(journal),
	// the stored state history will be truncated from head in the next restart.
	var (
		overflow bool
		oldest   uint64
	)
	if dl.db.freezer != nil {
		err := writeHistory(dl.db.freezer, bottom)
		if err != nil {
			return nil, err
		}
		// Determine if the persisted history object has exceeded the configured
		// limitation, set the overflow as true if so.
		tail, err := dl.db.freezer.Tail()
		if err != nil {
			return nil, err
		}
		limit := dl.db.config.StateHistory
		if limit != 0 && bottom.stateID()-tail > limit {
			overflow = true
			oldest = bottom.stateID() - limit + 1 // track the id of history **after truncation**
		}
	}
	// Mark the diskLayer as stale before applying any mutations on top.
	dl.stale = true

	// Store the root->id lookup afterwards. All stored lookups are identified
	// by the **unique** state root. It's impossible that in the same chain
	// blocks are not adjacent but have the same root.
	if dl.id == 0 {
		rawdb.WriteStateID(dl.db.diskdb, dl.root, 0)
	}
	rawdb.WriteStateID(dl.db.diskdb, bottom.rootHash(), bottom.stateID())

	// In a unique scenario where the ID of the oldest history object (after tail
	// truncation) surpasses the persisted state ID, we take the necessary action
	// of forcibly committing the cached dirty states to ensure that the persisted
	// state ID remains higher.
	if !force && rawdb.ReadPersistentStateID(dl.db.diskdb) < oldest {
		force = true
	}
	// Merge the trie nodes and flat states of the bottom-most diff layer into the
	// buffer as the combined layer.
	combined := dl.buffer.commit(bottom.nodes, bottom.states.stateSet)
	if err := combined.flush(dl.db.diskdb, dl.db.freezer, dl.nodes, bottom.stateID(), force); err != nil {
		return nil, err
	}
	ndl := newDiskLayer(bottom.root, bottom.stateID(), dl.db, dl.nodes, combined)

	// To remove outdated history objects from the end, we set the 'tail' parameter
	// to 'oldest-1' due to the offset between the freezer index and the history ID.
	if overflow {
		pruned, err := truncateFromTail(ndl.db.diskdb, ndl.db.freezer, oldest-1)
		if err != nil {
			return nil, err
		}
		log.Debug("Pruned state history", "items", pruned, "tailid", oldest)
	}

	// The bottom has been eaten by disklayer, releasing the hash cache of bottom difflayer.
	bottom.cache.Remove(bottom)
	return ndl, nil
}

// revert applies the given state history and return a reverted disk layer.
func (dl *diskLayer) revert(h *history) (*diskLayer, error) {
	if h.meta.root != dl.rootHash() {
		return nil, errUnexpectedHistory
	}
	if dl.id == 0 {
		return nil, fmt.Errorf("%w: zero state id", errStateUnrecoverable)
	}
	// Apply the reverse state changes upon the current state. This must
	// be done before holding the lock in order to access state in "this"
	// layer.
	nodes, err := apply(dl.db, h.meta.parent, h.meta.root, h.meta.version != stateHistoryV0, h.accounts, h.storages)
	if err != nil {
		return nil, err
	}
	// Derive the state modification set from the history, keyed by the hash
	// of the account address and the storage key.
	accounts, storages := h.stateSet()

	// Mark the diskLayer as stale before applying any mutations on top.
	dl.lock.Lock()
	defer dl.lock.Unlock()

	dl.stale = true

	// State change may be applied to node buffer, or the persistent
	// state, depends on if node buffer is empty or not. If the node
	// buffer is not empty, it means that the state transition that
	// needs to be reverted is not yet flushed and cached in node
	// buffer, otherwise, manipulate persistent state directly.
	if !dl.buffer.empty() {
		err := dl.buffer.revertTo(dl.db.diskdb, nodes, accounts, storages)
		if err != nil {
			return nil, err
		}
	} else {
		batch := dl.db.diskdb.NewBatch()
		writeNodes(batch, nodes, dl.nodes)
		rawdb.WritePersistentStateID(batch, dl.id-1)
		if err := batch.Write(); err != nil {
			log.Crit("Failed to write states", "err", err)
		}
	}
	return newDiskLayer(h.meta.parent, dl.id-1, dl.db, dl.nodes, dl.buffer), nil
}

// size returns the approximate size of cached nodes in the disk layer.
func (dl *diskLayer) size() (common.StorageSize, common.StorageSize) {
	dl.lock.RLock()
	defer dl.lock.RUnlock()

	if dl.stale {
		return 0, 0
	}
	dirtyNodes, dirtyimmutableNodes := dl.buffer.getSize()
	return common.StorageSize(dirtyNodes), common.StorageSize(dirtyimmutableNodes)
}

// resetCache releases the memory held by clean cache to prevent memory leak.
func (dl *diskLayer) resetCache() {
	dl.lock.RLock()
	defer dl.lock.RUnlock()

	// Stale disk layer loses the ownership of clean caches.
	if dl.stale {
		return
	}
	if dl.nodes != nil {
		dl.nodes.Reset()
	}
}

// hasher is used to compute the sha256 hash of the provided data.
type hasher struct{ sha crypto.KeccakState }

var hasherPool = sync.Pool{
	New: func() interface{} { return &hasher{sha: crypto.NewKeccakState()} },
}

func newHasher() *hasher {
	return hasherPool.Get().(*hasher)
}

func (h *hasher) hash(data []byte) common.Hash {
	return crypto.HashData(h.sha, data)
}

func (h *hasher) release() {
	hasherPool.Put(h)
}
