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
	"fmt"
	"time"

	"github.com/Ezkerrox/bsc/common"
	"github.com/Ezkerrox/bsc/core/rawdb"
	"github.com/Ezkerrox/bsc/ethdb"
	"github.com/Ezkerrox/bsc/log"
	"github.com/Ezkerrox/bsc/trie/trienode"
	"github.com/VictoriaMetrics/fastcache"
)

// buffer is a collection of modified states along with the modified trie nodes.
// They are cached here to aggregate the disk write. The content of the buffer
// must be checked before diving into disk (since it basically is not yet written
// data).
type buffer struct {
	layers uint64    // The number of diff layers aggregated inside
	limit  uint64    // The maximum memory allowance in bytes
	nodes  *nodeSet  // Aggregated trie node set
	states *stateSet // Aggregated state set
}

// newBuffer initializes the buffer with the provided states and trie nodes.
func newBuffer(limit int, nodes *nodeSet, states *stateSet, layers uint64) *buffer {
	// Don't panic for lazy users if any provided set is nil
	if nodes == nil {
		nodes = newNodeSet(nil)
	}
	if states == nil {
		states = newStates(nil, nil, false)
	}
	return &buffer{
		layers: layers,
		limit:  uint64(limit),
		nodes:  nodes,
		states: states,
	}
}

// account retrieves the account blob with account address hash.
func (b *buffer) account(hash common.Hash) ([]byte, bool) {
	return b.states.account(hash)
}

// storage retrieves the storage slot with account address hash and slot key.
func (b *buffer) storage(addrHash common.Hash, storageHash common.Hash) ([]byte, bool) {
	return b.states.storage(addrHash, storageHash)
}

// node retrieves the trie node with node path and its trie identifier.
func (b *buffer) node(owner common.Hash, path []byte) (*trienode.Node, bool) {
	return b.nodes.node(owner, path)
}

// commit merges the provided states and trie nodes into the buffer.
func (b *buffer) commit(nodes *nodeSet, states *stateSet) trienodebuffer {
	b.layers++
	b.nodes.merge(nodes)
	b.states.merge(states)
	return b
}

// revertTo is the reverse operation of commit. It also merges the provided states
// and trie nodes into the buffer. The key difference is that the provided state
// set should reverse the changes made by the most recent state transition.
func (b *buffer) revertTo(db ethdb.KeyValueReader, nodes map[common.Hash]map[string]*trienode.Node, accounts map[common.Hash][]byte, storages map[common.Hash]map[common.Hash][]byte) error {
	// Short circuit if no embedded state transition to revert
	if b.layers == 0 {
		return errStateUnrecoverable
	}
	b.layers--

	// Reset the entire buffer if only a single transition left
	if b.layers == 0 {
		b.reset()
		return nil
	}
	b.nodes.revertTo(db, nodes)
	// TODO(galaio): In order to be compatible with the legacy version, a temporary empty check is added,
	// which may affect the reading result of pbss as flatReader, see: flatReader.Account()
	// it could be removed in the future
	// Caution: there is also a panic issue with non-empty states when rewinding the chain again, but it is a very low possibility with finality.
	if len(b.states.accountData) != 0 || len(b.states.storageData) != 0 {
		b.states.revertTo(accounts, storages)
	}
	return nil
}

// reset cleans up the disk cache.
func (b *buffer) reset() {
	b.layers = 0
	b.nodes.reset()
	b.states.reset()
}

// empty returns an indicator if buffer is empty.
func (b *buffer) empty() bool {
	return b.layers == 0
}

// full returns an indicator if the size of accumulated content exceeds the
// configured threshold.
func (b *buffer) full() bool {
	return b.size() > b.limit
}

// size returns the approximate memory size of the held content.
func (b *buffer) size() uint64 {
	return b.states.size + b.nodes.size
}

// flush persists the in-memory dirty trie node into the disk if the configured
// memory threshold is reached. Note, all data must be written atomically.
func (b *buffer) flush(db ethdb.KeyValueStore, freezer ethdb.AncientWriter, nodesCache *fastcache.Cache, id uint64, force bool) error {
	if !b.full() && !force {
		return nil
	}

	// Ensure the target state id is aligned with the internal counter.
	head := rawdb.ReadPersistentStateID(db)
	if head+b.layers != id {
		return fmt.Errorf("buffer layers (%d) cannot be applied on top of persisted state id (%d) to reach requested state id (%d)", b.layers, head, id)
	}
	// Terminate the state snapshot generation if it's active
	var (
		start = time.Now()
		batch = db.NewBatchWithSize(b.nodes.dbsize() * 11 / 10) // extra 10% for potential pebble internal stuff
	)
	// Explicitly sync the state freezer to ensure all written data is persisted to disk
	// before updating the key-value store.
	//
	// This step is crucial to guarantee that the corresponding state history remains
	// available for state rollback.
	if freezer != nil {
		if err := freezer.SyncAncient(); err != nil {
			return err
		}
	}
	nodes := b.nodes.write(batch, nodesCache)
	rawdb.WritePersistentStateID(batch, id)

	// Flush all mutations in a single batch
	size := batch.ValueSize()
	if err := batch.Write(); err != nil {
		return err
	}
	commitBytesMeter.Mark(int64(size))
	commitNodesMeter.Mark(int64(nodes))
	commitTimeTimer.UpdateSince(start)
	b.reset()
	log.Debug("Persisted buffer content", "nodes", nodes, "bytes", common.StorageSize(size), "elapsed", common.PrettyDuration(time.Since(start)))
	return nil
}

func (b *buffer) waitAndStopFlushing() {}

// getAllNodesAndStates return the trie nodes and states cached in nodebuffer.
func (b *buffer) getAllNodesAndStates() (*nodeSet, *stateSet) {
	return b.nodes, b.states
}

func (b *buffer) getStates() *stateSet {
	return b.states
}

// getLayers return the size of cached difflayers.
func (b *buffer) getLayers() uint64 {
	return b.layers
}

// getSize return the nodebuffer used size.
func (b *buffer) getSize() (uint64, uint64) {
	return b.size(), 0
}
