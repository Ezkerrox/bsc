// Copyright 2015 The go-ethereum Authors
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

package fetcher

import (
	"errors"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ezkerrox/bsc/common"
	"github.com/Ezkerrox/bsc/consensus/ethash"
	"github.com/Ezkerrox/bsc/core"
	"github.com/Ezkerrox/bsc/core/rawdb"
	"github.com/Ezkerrox/bsc/core/types"
	"github.com/Ezkerrox/bsc/crypto"
	"github.com/Ezkerrox/bsc/eth/protocols/eth"
	"github.com/Ezkerrox/bsc/log"
	"github.com/Ezkerrox/bsc/params"
	"github.com/Ezkerrox/bsc/trie"
	"github.com/Ezkerrox/bsc/triedb"
)

var (
	testdb      = rawdb.NewMemoryDatabase()
	testKey, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	testAddress = crypto.PubkeyToAddress(testKey.PublicKey)
	gspec       = &core.Genesis{
		Config:  params.TestChainConfig,
		Alloc:   types.GenesisAlloc{testAddress: {Balance: big.NewInt(1000000000000000)}},
		BaseFee: big.NewInt(params.InitialBaseFee),
	}
	genesis      = gspec.MustCommit(testdb, triedb.NewDatabase(testdb, triedb.HashDefaults))
	unknownBlock = types.NewBlock(&types.Header{Root: types.EmptyRootHash, GasLimit: params.GenesisGasLimit, BaseFee: big.NewInt(params.InitialBaseFee)}, nil, nil, trie.NewStackTrie(nil))
)

// makeChain creates a chain of n blocks starting at and including parent.
// the returned hash chain is ordered head->parent. In addition, every 3rd block
// contains a transaction and every 5th an uncle to allow testing correct block
// reassembly.
func makeChain(n int, seed byte, parent *types.Block) ([]common.Hash, map[common.Hash]*types.Block) {
	blocks, _ := core.GenerateChain(gspec.Config, parent, ethash.NewFaker(), testdb, n, func(i int, block *core.BlockGen) {
		block.SetCoinbase(common.Address{seed})

		// If the block number is multiple of 3, send a bonus transaction to the miner
		if parent == genesis && i%3 == 0 {
			signer := types.MakeSigner(params.TestChainConfig, block.Number(), block.Timestamp())
			tx, err := types.SignTx(types.NewTransaction(block.TxNonce(testAddress), common.Address{seed}, big.NewInt(1000), params.TxGas, block.BaseFee(), nil), signer, testKey)
			if err != nil {
				panic(err)
			}
			block.AddTx(tx)
		}
		// If the block number is a multiple of 5, add a bonus uncle to the block
		if i > 0 && i%5 == 0 {
			block.AddUncle(&types.Header{ParentHash: block.PrevBlock(i - 2).Hash(), Number: big.NewInt(int64(i - 1))})
		}
	})
	hashes := make([]common.Hash, n+1)
	hashes[len(hashes)-1] = parent.Hash()
	blockm := make(map[common.Hash]*types.Block, n+1)
	blockm[parent.Hash()] = parent
	for i, b := range blocks {
		hashes[len(hashes)-i-2] = b.Hash()
		blockm[b.Hash()] = b
	}
	return hashes, blockm
}

// fetcherTester is a test simulator for mocking out local block chain.
type fetcherTester struct {
	fetcher *BlockFetcher

	hashes  []common.Hash                 // Hash chain belonging to the tester
	headers map[common.Hash]*types.Header // Headers belonging to the tester
	blocks  map[common.Hash]*types.Block  // Blocks belonging to the tester
	drops   map[string]bool               // Map of peers dropped by the fetcher

	lock sync.RWMutex
}

// newTester creates a new fetcher test mocker.
func newTester() *fetcherTester {
	tester := &fetcherTester{
		hashes:  []common.Hash{genesis.Hash()},
		headers: map[common.Hash]*types.Header{genesis.Hash(): genesis.Header()},
		blocks:  map[common.Hash]*types.Block{genesis.Hash(): genesis},
		drops:   make(map[string]bool),
	}
	tester.fetcher = NewBlockFetcher(tester.getBlock, tester.verifyHeader, tester.broadcastBlock,
		tester.chainHeight, tester.chainFinalizedHeight, tester.insertChain, tester.dropPeer,
		func(peer string, startHeight uint64, startHash common.Hash, count uint64) ([]*types.Block, error) {
			return nil, errors.New("not implemented")
		})
	tester.fetcher.Start()

	return tester
}

// getBlock retrieves a block from the tester's block chain.
func (f *fetcherTester) getBlock(hash common.Hash) *types.Block {
	f.lock.RLock()
	defer f.lock.RUnlock()

	return f.blocks[hash]
}

// verifyHeader is a nop placeholder for the block header verification.
func (f *fetcherTester) verifyHeader(header *types.Header) error {
	return nil
}

// broadcastBlock is a nop placeholder for the block broadcasting.
func (f *fetcherTester) broadcastBlock(block *types.Block, propagate bool) {
}

// chainHeight retrieves the current height (block number) of the chain.
func (f *fetcherTester) chainHeight() uint64 {
	f.lock.RLock()
	defer f.lock.RUnlock()

	return f.blocks[f.hashes[len(f.hashes)-1]].NumberU64()
}

func (f *fetcherTester) chainFinalizedHeight() uint64 {
	f.lock.RLock()
	defer f.lock.RUnlock()
	if len(f.hashes) < 3 {
		return 0
	}
	return f.blocks[f.hashes[len(f.hashes)-3]].NumberU64()
}

// insertChain injects a new blocks into the simulated chain.
func (f *fetcherTester) insertChain(blocks types.Blocks) (int, error) {
	f.lock.Lock()
	defer f.lock.Unlock()

	for i, block := range blocks {
		// Make sure the parent in known
		if _, ok := f.blocks[block.ParentHash()]; !ok {
			return i, errors.New("unknown parent")
		}
		// Discard any new blocks if the same height already exists
		if block.NumberU64() <= f.blocks[f.hashes[len(f.hashes)-1]].NumberU64() {
			return i, nil
		}
		// Otherwise build our current chain
		f.hashes = append(f.hashes, block.Hash())
		f.blocks[block.Hash()] = block
	}
	return 0, nil
}

// dropPeer is an emulator for the peer removal, simply accumulating the various
// peers dropped by the fetcher.
func (f *fetcherTester) dropPeer(peer string) {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.drops[peer] = true
}

// makeHeaderFetcher retrieves a block header fetcher associated with a simulated peer.
func (f *fetcherTester) makeHeaderFetcher(peer string, blocks map[common.Hash]*types.Block, drift time.Duration) headerRequesterFn {
	closure := make(map[common.Hash]*types.Block)
	for hash, block := range blocks {
		closure[hash] = block
	}
	// Create a function that return a header from the closure
	return func(hash common.Hash, sink chan *eth.Response) (*eth.Request, error) {
		// Gather the blocks to return
		headers := make([]*types.Header, 0, 1)
		if block, ok := closure[hash]; ok {
			headers = append(headers, block.Header())
		}
		// Return on a new thread
		req := &eth.Request{
			Peer: peer,
		}
		res := &eth.Response{
			Req:  req,
			Res:  (*eth.BlockHeadersRequest)(&headers),
			Time: drift,
			Done: make(chan error, 1), // Ignore the returned status
		}
		go func() {
			sink <- res
		}()
		return req, nil
	}
}

// makeBodyFetcher retrieves a block body fetcher associated with a simulated peer.
func (f *fetcherTester) makeBodyFetcher(peer string, blocks map[common.Hash]*types.Block, drift time.Duration) bodyRequesterFn {
	closure := make(map[common.Hash]*types.Block)
	for hash, block := range blocks {
		closure[hash] = block
	}
	// Create a function that returns blocks from the closure
	return func(hashes []common.Hash, sink chan *eth.Response) (*eth.Request, error) {
		// Gather the block bodies to return
		transactions := make([][]*types.Transaction, 0, len(hashes))
		uncles := make([][]*types.Header, 0, len(hashes))

		for _, hash := range hashes {
			if block, ok := closure[hash]; ok {
				transactions = append(transactions, block.Transactions())
				uncles = append(uncles, block.Uncles())
			}
		}
		// Return on a new thread
		bodies := make([]*eth.BlockBody, len(transactions))
		for i, txs := range transactions {
			bodies[i] = &eth.BlockBody{
				Transactions: txs,
				Uncles:       uncles[i],
			}
		}
		req := &eth.Request{
			Peer: peer,
		}
		res := &eth.Response{
			Req:  req,
			Res:  (*eth.BlockBodiesResponse)(&bodies),
			Time: drift,
			Done: make(chan error, 1), // Ignore the returned status
		}
		go func() {
			sink <- res
		}()
		return req, nil
	}
}

// verifyFetchingEvent verifies that one single event arrive on a fetching channel.
func verifyFetchingEvent(t *testing.T, fetching chan []common.Hash, arrive bool) {
	t.Helper()

	if arrive {
		select {
		case <-fetching:
		case <-time.After(time.Second):
			t.Fatalf("fetching timeout")
		}
	} else {
		select {
		case <-fetching:
			t.Fatalf("fetching invoked")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// verifyCompletingEvent verifies that one single event arrive on an completing channel.
func verifyCompletingEvent(t *testing.T, completing chan []common.Hash, arrive bool) {
	t.Helper()

	if arrive {
		select {
		case <-completing:
		case <-time.After(time.Second):
			t.Fatalf("completing timeout")
		}
	} else {
		select {
		case <-completing:
			t.Fatalf("completing invoked")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// verifyImportEvent verifies that one single event arrive on an import channel.
func verifyImportEvent(t *testing.T, imported chan interface{}, arrive bool) {
	t.Helper()

	if arrive {
		select {
		case <-imported:
		case <-time.After(time.Second):
			t.Fatalf("import timeout")
		}
	} else {
		select {
		case <-imported:
			t.Fatalf("import invoked")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// verifyImportCount verifies that exactly count number of events arrive on an
// import hook channel.
func verifyImportCount(t *testing.T, imported chan interface{}, count int) {
	t.Helper()

	for i := 0; i < count; i++ {
		select {
		case <-imported:
		case <-time.After(time.Second):
			t.Fatalf("block %d: import timeout", i+1)
		}
	}
	verifyImportDone(t, imported)
}

// verifyImportDone verifies that no more events are arriving on an import channel.
func verifyImportDone(t *testing.T, imported chan interface{}) {
	t.Helper()

	select {
	case <-imported:
		t.Fatalf("extra block imported")
	case <-time.After(50 * time.Millisecond):
	}
}

// verifyChainHeight verifies the chain height is as expected.
func verifyChainHeight(t *testing.T, fetcher *fetcherTester, height uint64) {
	t.Helper()

	if fetcher.chainHeight() != height {
		t.Fatalf("chain height mismatch, got %d, want %d", fetcher.chainHeight(), height)
	}
}

// Tests that a fetcher accepts block/header announcements and initiates retrievals
// for them, successfully importing into the local chain.
func TestFullSequentialAnnouncements(t *testing.T) { testSequentialAnnouncements(t) }

func testSequentialAnnouncements(t *testing.T) {
	// Create a chain of blocks to import
	targetBlocks := 4 * hashLimit
	hashes, blocks := makeChain(targetBlocks, 0, genesis)

	tester := newTester()
	defer tester.fetcher.Stop()
	headerFetcher := tester.makeHeaderFetcher("valid", blocks, -gatherSlack)
	bodyFetcher := tester.makeBodyFetcher("valid", blocks, 0)

	// Iteratively announce blocks until all are imported
	imported := make(chan interface{})
	tester.fetcher.importedHook = func(header *types.Header, block *types.Block) {
		if block == nil {
			t.Fatalf("Fetcher try to import empty block")
		}
		imported <- block
	}
	for i := len(hashes) - 2; i >= 0; i-- {
		tester.fetcher.Notify("valid", hashes[i], uint64(len(hashes)-i-1), time.Now().Add(-arriveTimeout), headerFetcher, bodyFetcher)
		verifyImportEvent(t, imported, true)
	}
	verifyImportDone(t, imported)
	verifyChainHeight(t, tester, uint64(len(hashes)-1))
}

// Tests that if blocks are announced by multiple peers (or even the same buggy
// peer), they will only get downloaded at most once.
func TestFullConcurrentAnnouncements(t *testing.T) { testConcurrentAnnouncements(t) }

func testConcurrentAnnouncements(t *testing.T) {
	// Create a chain of blocks to import
	targetBlocks := 4 * hashLimit
	hashes, blocks := makeChain(targetBlocks, 0, genesis)

	// Assemble a tester with a built in counter for the requests
	tester := newTester()
	firstHeaderFetcher := tester.makeHeaderFetcher("first", blocks, -gatherSlack)
	firstBodyFetcher := tester.makeBodyFetcher("first", blocks, 0)
	secondHeaderFetcher := tester.makeHeaderFetcher("second", blocks, -gatherSlack)
	secondBodyFetcher := tester.makeBodyFetcher("second", blocks, 0)

	var counter atomic.Uint32
	firstHeaderWrapper := func(hash common.Hash, sink chan *eth.Response) (*eth.Request, error) {
		counter.Add(1)
		return firstHeaderFetcher(hash, sink)
	}
	secondHeaderWrapper := func(hash common.Hash, sink chan *eth.Response) (*eth.Request, error) {
		counter.Add(1)
		return secondHeaderFetcher(hash, sink)
	}
	// Iteratively announce blocks until all are imported
	imported := make(chan interface{})
	tester.fetcher.importedHook = func(header *types.Header, block *types.Block) {
		if block == nil {
			t.Fatalf("Fetcher try to import empty block")
		}
		imported <- block
	}
	for i := len(hashes) - 2; i >= 0; i-- {
		tester.fetcher.Notify("first", hashes[i], uint64(len(hashes)-i-1), time.Now().Add(-arriveTimeout), firstHeaderWrapper, firstBodyFetcher)
		tester.fetcher.Notify("second", hashes[i], uint64(len(hashes)-i-1), time.Now().Add(-arriveTimeout+time.Millisecond), secondHeaderWrapper, secondBodyFetcher)
		tester.fetcher.Notify("second", hashes[i], uint64(len(hashes)-i-1), time.Now().Add(-arriveTimeout-time.Millisecond), secondHeaderWrapper, secondBodyFetcher)
		verifyImportEvent(t, imported, true)
	}
	verifyImportDone(t, imported)

	// Make sure no blocks were retrieved twice
	if c := int(counter.Load()); c != targetBlocks {
		t.Fatalf("retrieval count mismatch: have %v, want %v", c, targetBlocks)
	}
	verifyChainHeight(t, tester, uint64(len(hashes)-1))
}

// Tests that announcements arriving while a previous is being fetched still
// results in a valid import.
func TestFullOverlappingAnnouncements(t *testing.T) { testOverlappingAnnouncements(t) }

func testOverlappingAnnouncements(t *testing.T) {
	// Create a chain of blocks to import
	targetBlocks := 4 * hashLimit
	hashes, blocks := makeChain(targetBlocks, 0, genesis)

	tester := newTester()
	headerFetcher := tester.makeHeaderFetcher("valid", blocks, -gatherSlack)
	bodyFetcher := tester.makeBodyFetcher("valid", blocks, 0)

	// Iteratively announce blocks, but overlap them continuously
	overlap := 16
	imported := make(chan interface{}, len(hashes)-1)
	for i := 0; i < overlap; i++ {
		imported <- nil
	}
	tester.fetcher.importedHook = func(header *types.Header, block *types.Block) {
		if block == nil {
			t.Fatalf("Fetcher try to import empty block")
		}
		imported <- block
	}

	for i := len(hashes) - 2; i >= 0; i-- {
		tester.fetcher.Notify("valid", hashes[i], uint64(len(hashes)-i-1), time.Now().Add(-arriveTimeout), headerFetcher, bodyFetcher)
		select {
		case <-imported:
		case <-time.After(time.Second):
			t.Fatalf("block %d: import timeout", len(hashes)-i)
		}
	}
	// Wait for all the imports to complete and check count
	verifyImportCount(t, imported, overlap)
	verifyChainHeight(t, tester, uint64(len(hashes)-1))
}

// Tests that announces already being retrieved will not be duplicated.
func TestFullPendingDeduplication(t *testing.T) { testPendingDeduplication(t) }

func testPendingDeduplication(t *testing.T) {
	// Create a hash and corresponding block
	hashes, blocks := makeChain(1, 0, genesis)

	// Assemble a tester with a built in counter and delayed fetcher
	tester := newTester()
	headerFetcher := tester.makeHeaderFetcher("repeater", blocks, -gatherSlack)
	bodyFetcher := tester.makeBodyFetcher("repeater", blocks, 0)

	delay := 50 * time.Millisecond
	var counter atomic.Uint32
	headerWrapper := func(hash common.Hash, sink chan *eth.Response) (*eth.Request, error) {
		counter.Add(1)

		// Simulate a long running fetch
		resink := make(chan *eth.Response)
		req, err := headerFetcher(hash, resink)
		if err == nil {
			go func() {
				res := <-resink
				time.Sleep(delay)
				sink <- res
			}()
		}
		return req, err
	}
	checkNonExist := func() bool {
		return tester.getBlock(hashes[0]) == nil
	}
	// Announce the same block many times until it's fetched (wait for any pending ops)
	for checkNonExist() {
		tester.fetcher.Notify("repeater", hashes[0], 1, time.Now().Add(-arriveTimeout), headerWrapper, bodyFetcher)
		time.Sleep(time.Millisecond)
	}
	time.Sleep(delay)

	// Check that all blocks were imported and none fetched twice
	if c := counter.Load(); c != 1 {
		t.Fatalf("retrieval count mismatch: have %v, want %v", c, 1)
	}
	verifyChainHeight(t, tester, 1)
}

// Tests that announcements retrieved in a random order are cached and eventually
// imported when all the gaps are filled in.
func TestFullRandomArrivalImport(t *testing.T) { testRandomArrivalImport(t) }

func testRandomArrivalImport(t *testing.T) {
	// Create a chain of blocks to import, and choose one to delay
	targetBlocks := maxQueueDist
	hashes, blocks := makeChain(targetBlocks, 0, genesis)
	skip := targetBlocks / 2

	tester := newTester()
	headerFetcher := tester.makeHeaderFetcher("valid", blocks, -gatherSlack)
	bodyFetcher := tester.makeBodyFetcher("valid", blocks, 0)

	// Iteratively announce blocks, skipping one entry
	imported := make(chan interface{}, len(hashes)-1)
	tester.fetcher.importedHook = func(header *types.Header, block *types.Block) {
		if block == nil {
			t.Fatalf("Fetcher try to import empty block")
		}
		imported <- block
	}
	for i := len(hashes) - 1; i >= 0; i-- {
		if i != skip {
			tester.fetcher.Notify("valid", hashes[i], uint64(len(hashes)-i-1), time.Now().Add(-arriveTimeout), headerFetcher, bodyFetcher)
			time.Sleep(time.Millisecond)
		}
	}
	// Finally announce the skipped entry and check full import
	tester.fetcher.Notify("valid", hashes[skip], uint64(len(hashes)-skip-1), time.Now().Add(-arriveTimeout), headerFetcher, bodyFetcher)
	verifyImportCount(t, imported, len(hashes)-1)
	verifyChainHeight(t, tester, uint64(len(hashes)-1))
}

// Tests that direct block enqueues (due to block propagation vs. hash announce)
// are correctly schedule, filling and import queue gaps.
func TestQueueGapFill(t *testing.T) {
	// Create a chain of blocks to import, and choose one to not announce at all
	targetBlocks := maxQueueDist
	hashes, blocks := makeChain(targetBlocks, 0, genesis)
	skip := targetBlocks / 2

	tester := newTester()
	headerFetcher := tester.makeHeaderFetcher("valid", blocks, -gatherSlack)
	bodyFetcher := tester.makeBodyFetcher("valid", blocks, 0)

	// Iteratively announce blocks, skipping one entry
	imported := make(chan interface{}, len(hashes)-1)
	tester.fetcher.importedHook = func(header *types.Header, block *types.Block) { imported <- block }

	for i := len(hashes) - 1; i >= 0; i-- {
		if i != skip {
			tester.fetcher.Notify("valid", hashes[i], uint64(len(hashes)-i-1), time.Now().Add(-arriveTimeout), headerFetcher, bodyFetcher)
			time.Sleep(time.Millisecond)
		}
	}
	// Fill the missing block directly as if propagated
	tester.fetcher.Enqueue("valid", blocks[hashes[skip]])
	verifyImportCount(t, imported, len(hashes)-1)
	verifyChainHeight(t, tester, uint64(len(hashes)-1))
}

// Tests that blocks arriving from various sources (multiple propagations, hash
// announces, etc) do not get scheduled for import multiple times.
func TestImportDeduplication(t *testing.T) {
	// Create two blocks to import (one for duplication, the other for stalling)
	hashes, blocks := makeChain(2, 0, genesis)

	// Create the tester and wrap the importer with a counter
	tester := newTester()
	headerFetcher := tester.makeHeaderFetcher("valid", blocks, -gatherSlack)
	bodyFetcher := tester.makeBodyFetcher("valid", blocks, 0)

	var counter atomic.Uint32
	tester.fetcher.insertChain = func(blocks types.Blocks) (int, error) {
		counter.Add(uint32(len(blocks)))
		return tester.insertChain(blocks)
	}
	// Instrument the fetching and imported events
	fetching := make(chan []common.Hash)
	imported := make(chan interface{}, len(hashes)-1)
	tester.fetcher.fetchingHook = func(hashes []common.Hash) { fetching <- hashes }
	tester.fetcher.importedHook = func(header *types.Header, block *types.Block) { imported <- block }

	// Announce the duplicating block, wait for retrieval, and also propagate directly
	tester.fetcher.Notify("valid", hashes[0], 1, time.Now().Add(-arriveTimeout), headerFetcher, bodyFetcher)
	<-fetching

	tester.fetcher.Enqueue("valid", blocks[hashes[0]])
	tester.fetcher.Enqueue("valid", blocks[hashes[0]])
	tester.fetcher.Enqueue("valid", blocks[hashes[0]])

	// Fill the missing block directly as if propagated, and check import uniqueness
	tester.fetcher.Enqueue("valid", blocks[hashes[1]])
	verifyImportCount(t, imported, 2)

	if c := counter.Load(); c != 2 {
		t.Fatalf("import invocation count mismatch: have %v, want %v", c, 2)
	}
}

// Tests that blocks with numbers much lower or higher than out current head get
// discarded to prevent wasting resources on useless blocks from faulty peers.
func TestDistantPropagationDiscarding(t *testing.T) {
	// Create a long chain to import and define the discard boundaries
	hashes, blocks := makeChain(3*maxQueueDist, 0, genesis)
	head := hashes[len(hashes)/2]

	low, high := len(hashes)/2+maxUncleDist+1, len(hashes)/2-maxQueueDist-1

	// Create a tester and simulate a head block being the middle of the above chain
	tester := newTester()

	tester.lock.Lock()
	tester.hashes = []common.Hash{head}
	tester.blocks = map[common.Hash]*types.Block{head: blocks[head]}
	tester.lock.Unlock()

	// Ensure that a block with a lower number than the threshold is discarded
	tester.fetcher.Enqueue("lower", blocks[hashes[low]])
	time.Sleep(10 * time.Millisecond)
	if !tester.fetcher.queue.Empty() {
		t.Fatalf("fetcher queued stale block")
	}
	// Ensure that a block with a higher number than the threshold is discarded
	tester.fetcher.Enqueue("higher", blocks[hashes[high]])
	time.Sleep(10 * time.Millisecond)
	if !tester.fetcher.queue.Empty() {
		t.Fatalf("fetcher queued future block")
	}
}

// Tests that announcements with numbers much lower or higher than out current
// head get discarded to prevent wasting resources on useless blocks from faulty
// peers.
func TestFullDistantAnnouncementDiscarding(t *testing.T) { testDistantAnnouncementDiscarding(t) }

func testDistantAnnouncementDiscarding(t *testing.T) {
	// Create a long chain to import and define the discard boundaries
	hashes, blocks := makeChain(3*maxQueueDist, 0, genesis)
	head := hashes[len(hashes)/2]

	low, high := len(hashes)/2+maxUncleDist+1, len(hashes)/2-maxQueueDist-1

	// Create a tester and simulate a head block being the middle of the above chain
	tester := newTester()

	tester.lock.Lock()
	tester.hashes = []common.Hash{head}
	tester.headers = map[common.Hash]*types.Header{head: blocks[head].Header()}
	tester.blocks = map[common.Hash]*types.Block{head: blocks[head]}
	tester.lock.Unlock()

	headerFetcher := tester.makeHeaderFetcher("lower", blocks, -gatherSlack)
	bodyFetcher := tester.makeBodyFetcher("lower", blocks, 0)

	fetching := make(chan struct{}, 2)
	tester.fetcher.fetchingHook = func(hashes []common.Hash) { fetching <- struct{}{} }

	// Ensure that a block with a lower number than the threshold is discarded
	tester.fetcher.Notify("lower", hashes[low], blocks[hashes[low]].NumberU64(), time.Now().Add(-arriveTimeout), headerFetcher, bodyFetcher)
	select {
	case <-time.After(50 * time.Millisecond):
	case <-fetching:
		t.Fatalf("fetcher requested stale header")
	}
	// Ensure that a block with a higher number than the threshold is discarded
	tester.fetcher.Notify("higher", hashes[high], blocks[hashes[high]].NumberU64(), time.Now().Add(-arriveTimeout), headerFetcher, bodyFetcher)
	select {
	case <-time.After(50 * time.Millisecond):
	case <-fetching:
		t.Fatalf("fetcher requested future header")
	}
}

// Tests that announcements with numbers much lower or equal to the current finalized block
// head get discarded to prevent wasting resources on useless blocks from faulty peers.
func TestFullFinalizedAnnouncementDiscarding(t *testing.T) {
	testFinalizedAnnouncementDiscarding(t)
}

func testFinalizedAnnouncementDiscarding(t *testing.T) {
	// Create a long chain to import and define the discard boundaries
	hashes, blocks := makeChain(3*maxQueueDist, 0, genesis)

	head := hashes[len(hashes)/2]
	justified := hashes[len(hashes)/2+1]
	finalized := hashes[len(hashes)/2+2]
	beforeFinalized := hashes[len(hashes)/2+3]

	low, equal := len(hashes)/2+3, len(hashes)/2+2

	// Create a tester and simulate a head block being the middle of the above chain
	tester := newTester()

	tester.lock.Lock()
	tester.hashes = []common.Hash{beforeFinalized, finalized, justified, head}
	tester.headers = map[common.Hash]*types.Header{
		beforeFinalized: blocks[beforeFinalized].Header(),
		finalized:       blocks[finalized].Header(),
		justified:       blocks[justified].Header(),
		head:            blocks[head].Header(),
	}
	tester.blocks = map[common.Hash]*types.Block{
		beforeFinalized: blocks[beforeFinalized],
		finalized:       blocks[finalized],
		justified:       blocks[justified],
		head:            blocks[head],
	}
	tester.lock.Unlock()

	headerFetcher := tester.makeHeaderFetcher("lower", blocks, -gatherSlack)
	bodyFetcher := tester.makeBodyFetcher("lower", blocks, 0)

	fetching := make(chan struct{}, 2)
	tester.fetcher.fetchingHook = func(hashes []common.Hash) { fetching <- struct{}{} }

	// Ensure that a block with a lower number than the finalized height is discarded
	tester.fetcher.Notify("lower", hashes[low], blocks[hashes[low]].NumberU64(), time.Now().Add(-arriveTimeout), headerFetcher, bodyFetcher)
	select {
	case <-time.After(50 * time.Millisecond):
	case <-fetching:
		t.Fatalf("fetcher requested stale header")
	}
	// Ensure that a block with a same number of the finalized height is discarded
	tester.fetcher.Notify("equal", hashes[equal], blocks[hashes[equal]].NumberU64(), time.Now().Add(-arriveTimeout), headerFetcher, bodyFetcher)
	select {
	case <-time.After(50 * time.Millisecond):
	case <-fetching:
		t.Fatalf("fetcher requested future header")
	}
}

// Tests that peers announcing blocks with invalid numbers (i.e. not matching
// the headers provided afterwards) get dropped as malicious.
func TestFullInvalidNumberAnnouncement(t *testing.T) { testInvalidNumberAnnouncement(t) }

func testInvalidNumberAnnouncement(t *testing.T) {
	// Create a single block to import and check numbers against
	hashes, blocks := makeChain(1, 0, genesis)

	tester := newTester()
	badHeaderFetcher := tester.makeHeaderFetcher("bad", blocks, -gatherSlack)
	badBodyFetcher := tester.makeBodyFetcher("bad", blocks, 0)

	imported := make(chan interface{})
	announced := make(chan interface{}, 2)
	tester.fetcher.importedHook = func(header *types.Header, block *types.Block) {
		if block == nil {
			t.Fatalf("Fetcher try to import empty block")
		}
		imported <- block
	}
	// Announce a block with a bad number, check for immediate drop
	tester.fetcher.announceChangeHook = func(hash common.Hash, b bool) {
		announced <- nil
	}
	tester.fetcher.Notify("bad", hashes[0], 2, time.Now().Add(-arriveTimeout), badHeaderFetcher, badBodyFetcher)
	verifyAnnounce := func() {
		for i := 0; i < 2; i++ {
			select {
			case <-announced:
				continue
			case <-time.After(1 * time.Second):
				t.Fatal("announce timeout")
			}
		}
	}
	verifyAnnounce()
	verifyImportEvent(t, imported, false)
	tester.lock.RLock()
	dropped := tester.drops["bad"]
	tester.lock.RUnlock()

	if !dropped {
		t.Fatalf("peer with invalid numbered announcement not dropped")
	}
	goodHeaderFetcher := tester.makeHeaderFetcher("good", blocks, -gatherSlack)
	goodBodyFetcher := tester.makeBodyFetcher("good", blocks, 0)
	// Make sure a good announcement passes without a drop
	tester.fetcher.Notify("good", hashes[0], 1, time.Now().Add(-arriveTimeout), goodHeaderFetcher, goodBodyFetcher)
	verifyAnnounce()
	verifyImportEvent(t, imported, true)

	tester.lock.RLock()
	dropped = tester.drops["good"]
	tester.lock.RUnlock()

	if dropped {
		t.Fatalf("peer with valid numbered announcement dropped")
	}
	verifyImportDone(t, imported)
}

// Tests that if a block is empty (i.e. header only), no body request should be
// made, and instead the header should be assembled into a whole block in itself.
func TestEmptyBlockShortCircuit(t *testing.T) {
	// Create a chain of blocks to import
	hashes, blocks := makeChain(32, 0, genesis)

	tester := newTester()
	defer tester.fetcher.Stop()
	headerFetcher := tester.makeHeaderFetcher("valid", blocks, -gatherSlack)
	bodyFetcher := tester.makeBodyFetcher("valid", blocks, 0)

	// Add a monitoring hook for all internal events
	fetching := make(chan []common.Hash)
	tester.fetcher.fetchingHook = func(hashes []common.Hash) { fetching <- hashes }

	completing := make(chan []common.Hash)
	tester.fetcher.completingHook = func(hashes []common.Hash) { completing <- hashes }

	imported := make(chan interface{})
	tester.fetcher.importedHook = func(header *types.Header, block *types.Block) {
		if block == nil {
			t.Fatalf("Fetcher try to import empty block")
		}
		imported <- block
	}
	// Iteratively announce blocks until all are imported
	for i := len(hashes) - 2; i >= 0; i-- {
		tester.fetcher.Notify("valid", hashes[i], uint64(len(hashes)-i-1), time.Now().Add(-arriveTimeout), headerFetcher, bodyFetcher)

		// All announces should fetch the header
		verifyFetchingEvent(t, fetching, true)

		// Only blocks with data contents should request bodies
		verifyCompletingEvent(t, completing, len(blocks[hashes[i]].Transactions()) > 0 || len(blocks[hashes[i]].Uncles()) > 0)

		// Irrelevant of the construct, import should succeed
		verifyImportEvent(t, imported, true)
	}
	verifyImportDone(t, imported)
}

// Tests that a peer is unable to use unbounded memory with sending infinite
// block announcements to a node, but that even in the face of such an attack,
// the fetcher remains operational.
func TestHashMemoryExhaustionAttack(t *testing.T) {
	// Create a tester with instrumented import hooks
	tester := newTester()

	imported, announces := make(chan interface{}), atomic.Int32{}
	tester.fetcher.importedHook = func(header *types.Header, block *types.Block) { imported <- block }
	tester.fetcher.announceChangeHook = func(hash common.Hash, added bool) {
		if added {
			announces.Add(1)
		} else {
			announces.Add(-1)
		}
	}
	// Create a valid chain and an infinite junk chain
	targetBlocks := hashLimit + 2*maxQueueDist
	hashes, blocks := makeChain(targetBlocks, 0, genesis)
	validHeaderFetcher := tester.makeHeaderFetcher("valid", blocks, -gatherSlack)
	validBodyFetcher := tester.makeBodyFetcher("valid", blocks, 0)

	attack, _ := makeChain(targetBlocks, 0, unknownBlock)
	attackerHeaderFetcher := tester.makeHeaderFetcher("attacker", nil, -gatherSlack)
	attackerBodyFetcher := tester.makeBodyFetcher("attacker", nil, 0)

	// Feed the tester a huge hashset from the attacker, and a limited from the valid peer
	for i := 0; i < len(attack); i++ {
		if i < maxQueueDist {
			tester.fetcher.Notify("valid", hashes[len(hashes)-2-i], uint64(i+1), time.Now(), validHeaderFetcher, validBodyFetcher)
		}
		tester.fetcher.Notify("attacker", attack[i], 1 /* don't distance drop */, time.Now(), attackerHeaderFetcher, attackerBodyFetcher)
	}
	if count := announces.Load(); count != hashLimit+maxQueueDist {
		t.Fatalf("queued announce count mismatch: have %d, want %d", count, hashLimit+maxQueueDist)
	}
	// Wait for fetches to complete
	verifyImportCount(t, imported, maxQueueDist)

	// Feed the remaining valid hashes to ensure DOS protection state remains clean
	for i := len(hashes) - maxQueueDist - 2; i >= 0; i-- {
		tester.fetcher.Notify("valid", hashes[i], uint64(len(hashes)-i-1), time.Now().Add(-arriveTimeout), validHeaderFetcher, validBodyFetcher)
		verifyImportEvent(t, imported, true)
	}
	verifyImportDone(t, imported)
}

// Tests that blocks sent to the fetcher (either through propagation or via hash
// announces and retrievals) don't pile up indefinitely, exhausting available
// system memory.
func TestBlockMemoryExhaustionAttack(t *testing.T) {
	// Create a tester with instrumented import hooks
	tester := newTester()

	imported, enqueued := make(chan interface{}), atomic.Int32{}
	tester.fetcher.importedHook = func(header *types.Header, block *types.Block) { imported <- block }
	tester.fetcher.queueChangeHook = func(hash common.Hash, added bool) {
		if added {
			enqueued.Add(1)
		} else {
			enqueued.Add(-1)
		}
	}
	// Create a valid chain and a batch of dangling (but in range) blocks
	targetBlocks := hashLimit + 2*maxQueueDist
	hashes, blocks := makeChain(targetBlocks, 0, genesis)
	attack := make(map[common.Hash]*types.Block)
	for i := byte(0); len(attack) < blockLimit+2*maxQueueDist; i++ {
		hashes, blocks := makeChain(maxQueueDist-1, i, unknownBlock)
		for _, hash := range hashes[:maxQueueDist-2] {
			attack[hash] = blocks[hash]
		}
	}
	// Try to feed all the attacker blocks make sure only a limited batch is accepted
	for _, block := range attack {
		tester.fetcher.Enqueue("attacker", block)
	}
	time.Sleep(200 * time.Millisecond)
	if queued := enqueued.Load(); queued != blockLimit {
		t.Fatalf("queued block count mismatch: have %d, want %d", queued, blockLimit)
	}
	// Queue up a batch of valid blocks, and check that a new peer is allowed to do so
	for i := 0; i < maxQueueDist-1; i++ {
		tester.fetcher.Enqueue("valid", blocks[hashes[len(hashes)-3-i]])
	}
	time.Sleep(100 * time.Millisecond)
	if queued := enqueued.Load(); queued != blockLimit+maxQueueDist-1 {
		t.Fatalf("queued block count mismatch: have %d, want %d", queued, blockLimit+maxQueueDist-1)
	}
	// Insert the missing piece (and sanity check the import)
	tester.fetcher.Enqueue("valid", blocks[hashes[len(hashes)-2]])
	verifyImportCount(t, imported, maxQueueDist)

	// Insert the remaining blocks in chunks to ensure clean DOS protection
	for i := maxQueueDist; i < len(hashes)-1; i++ {
		tester.fetcher.Enqueue("valid", blocks[hashes[len(hashes)-2-i]])
		verifyImportEvent(t, imported, true)
	}
	verifyImportDone(t, imported)
}

// mockBlockRetriever simulates block retrieval from the local chain
type mockBlockRetriever struct {
	blocks map[common.Hash]*types.Block
}

func newMockBlockRetriever() *mockBlockRetriever {
	return &mockBlockRetriever{
		blocks: make(map[common.Hash]*types.Block),
	}
}

func (m *mockBlockRetriever) getBlock(hash common.Hash) *types.Block {
	return m.blocks[hash]
}

// mockHeaderRequester simulates header requests
type mockHeaderRequester struct {
	headers map[common.Hash]*types.Header
	delay   time.Duration
}

func newMockHeaderRequester(delay time.Duration) *mockHeaderRequester {
	return &mockHeaderRequester{
		headers: make(map[common.Hash]*types.Header),
		delay:   delay,
	}
}

func (m *mockHeaderRequester) requestHeader(hash common.Hash, ch chan *eth.Response) (*eth.Request, error) {
	go func() {
		time.Sleep(m.delay)
		if header, ok := m.headers[hash]; ok {
			ch <- &eth.Response{
				Res: &eth.BlockHeadersRequest{header},
			}
		} else {
			ch <- &eth.Response{
				Res: &eth.BlockHeadersRequest{},
			}
		}
	}()
	return &eth.Request{}, nil
}

// mockBodyRequester simulates body requests
type mockBodyRequester struct {
	bodies map[common.Hash]*types.Body
	delay  time.Duration
}

func newMockBodyRequester(delay time.Duration) *mockBodyRequester {
	return &mockBodyRequester{
		bodies: make(map[common.Hash]*types.Body),
		delay:  delay,
	}
}

func (m *mockBodyRequester) requestBodies(hashes []common.Hash, ch chan *eth.Response) (*eth.Request, error) {
	go func() {
		time.Sleep(m.delay)
		var bodies []*eth.BlockBody
		for _, hash := range hashes {
			if body, ok := m.bodies[hash]; ok {
				bodies = append(bodies, &eth.BlockBody{
					Transactions: body.Transactions,
					Uncles:       body.Uncles,
				})
			}
		}
		ch <- &eth.Response{
			Res: (*eth.BlockBodiesResponse)(&bodies),
		}
	}()
	return &eth.Request{}, nil
}

// TestBlockFetcherMultiplePeers tests block synchronization between multiple peers
func TestBlockFetcherMultiplePeers(t *testing.T) {
	// Setup test environment
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stdout, log.LevelTrace, true)))

	// Create test blocks
	parent := types.NewBlock(&types.Header{
		Number:     big.NewInt(1),
		ParentHash: common.Hash{},
	}, nil, nil, nil)
	block := types.NewBlock(&types.Header{
		Number:     big.NewInt(2),
		ParentHash: parent.Hash(),
	}, nil, nil, nil)

	// Create block storage
	blockStore := make(map[common.Hash]*types.Block)
	blockStore[parent.Hash()] = parent

	// Create fetcher
	fetcher := NewBlockFetcher(
		// getBlock
		func(hash common.Hash) *types.Block {
			return blockStore[hash]
		},
		// verifyHeader
		func(header *types.Header) error {
			return nil
		},
		// broadcastBlock
		func(block *types.Block, propagate bool) {},
		// chainHeight - returns the height of the highest block in the chain
		func() uint64 {
			var maxHeight uint64 = 0
			for _, block := range blockStore {
				height := block.NumberU64()
				if height > maxHeight {
					maxHeight = height
				}
			}
			return maxHeight
		},
		// chainFinalizedHeight
		func() uint64 { return 0 },
		// insertChain
		func(blocks types.Blocks) (int, error) {
			for _, b := range blocks {
				blockStore[b.Hash()] = b
			}
			return len(blocks), nil
		},
		// dropPeer
		func(id string) {},
		// fetchRangeBlocks
		func(peer string, startHeight uint64, startHash common.Hash, count uint64) ([]*types.Block, error) {
			return nil, errors.New("not implemented")
		},
	)

	// Start fetcher
	fetcher.Start()
	defer fetcher.Stop()

	// Test case 1: Normal download process
	t.Run("normal download", func(t *testing.T) {
		// Create request functions
		headerRequester := func(hash common.Hash, sink chan *eth.Response) (*eth.Request, error) {
			go func() {
				// Return requested header
				headers := []*types.Header{block.Header()}
				res := &eth.Response{
					Req:  &eth.Request{},
					Res:  (*eth.BlockHeadersRequest)(&headers),
					Done: make(chan error, 1),
				}
				sink <- res
			}()
			return &eth.Request{}, nil
		}

		bodyRequester := func(hashes []common.Hash, sink chan *eth.Response) (*eth.Request, error) {
			go func() {
				// Return requested body
				bodies := make([]*eth.BlockBody, 0)
				for _, hash := range hashes {
					if hash == block.Hash() {
						bodies = append(bodies, &eth.BlockBody{
							Transactions: block.Transactions(),
							Uncles:       block.Uncles(),
						})
					}
				}
				res := &eth.Response{
					Req:  &eth.Request{},
					Res:  (*eth.BlockBodiesResponse)(&bodies),
					Done: make(chan error, 1),
				}
				sink <- res
			}()
			return &eth.Request{}, nil
		}

		// Peer1 sends block notification
		err := fetcher.Notify("peer1", block.Hash(), block.NumberU64(), time.Now(),
			headerRequester, bodyRequester)
		if err != nil {
			t.Fatalf("Notify failed: %v", err)
		}

		// Wait for the block to be processed
		for i := 0; i < 20; i++ {
			time.Sleep(50 * time.Millisecond)
			if blockStore[block.Hash()] != nil {
				break
			}
		}

		// Verify if the block was downloaded correctly
		if fetchedBlock := blockStore[block.Hash()]; fetchedBlock == nil {
			t.Error("Block was not downloaded")
		}
	})

	// Test case 2: Download timeout
	t.Run("download timeout", func(t *testing.T) {
		// Create a header requester with timeout
		slowHeaderRequester := func(hash common.Hash, sink chan *eth.Response) (*eth.Request, error) {
			go func() {
				// Intentionally not returning any content, simulating timeout
				time.Sleep(2 * fetchTimeout)
			}()
			return &eth.Request{}, nil
		}

		bodyRequester := func(hashes []common.Hash, sink chan *eth.Response) (*eth.Request, error) {
			go func() {
				// This won't be called
			}()
			return &eth.Request{}, nil
		}

		// Peer2 sends block notification
		err := fetcher.Notify("peer2", block.Hash(), block.NumberU64(), time.Now(),
			slowHeaderRequester, bodyRequester)
		if err != nil {
			t.Fatalf("Notify failed: %v", err)
		}

		// Wait for timeout
		time.Sleep(fetchTimeout + 100*time.Millisecond)
	})

	// Test case 3: Simplified single block notification test
	t.Run("single block announcement", func(t *testing.T) {
		// Create a new block
		newBlock := types.NewBlock(&types.Header{
			Number:     big.NewInt(3),
			ParentHash: block.Hash(), // Parent block is from the previous test
		}, nil, nil, nil)

		// Create request functions
		headerRequester := func(hash common.Hash, sink chan *eth.Response) (*eth.Request, error) {
			go func() {
				// Return requested header
				headers := []*types.Header{newBlock.Header()}
				res := &eth.Response{
					Req:  &eth.Request{},
					Res:  (*eth.BlockHeadersRequest)(&headers),
					Done: make(chan error, 1),
				}
				sink <- res
			}()
			return &eth.Request{}, nil
		}

		bodyRequester := func(hashes []common.Hash, sink chan *eth.Response) (*eth.Request, error) {
			go func() {
				// Return requested body
				bodies := make([]*eth.BlockBody, 0)
				for _, hash := range hashes {
					if hash == newBlock.Hash() {
						bodies = append(bodies, &eth.BlockBody{
							Transactions: newBlock.Transactions(),
							Uncles:       newBlock.Uncles(),
						})
					}
				}
				res := &eth.Response{
					Req:  &eth.Request{},
					Res:  (*eth.BlockBodiesResponse)(&bodies),
					Done: make(chan error, 1),
				}
				sink <- res
			}()
			return &eth.Request{}, nil
		}

		// Send block notification
		err := fetcher.Notify("peer1", newBlock.Hash(), newBlock.NumberU64(), time.Now(),
			headerRequester, bodyRequester)
		if err != nil {
			t.Fatalf("Notify failed: %v", err)
		}

		// Wait for the block to be processed
		for i := 0; i < 20; i++ {
			time.Sleep(50 * time.Millisecond)
			if blockStore[newBlock.Hash()] != nil {
				break
			}
		}

		// Verify if the block was downloaded correctly
		if fetchedBlock := blockStore[newBlock.Hash()]; fetchedBlock == nil {
			t.Error("New block was not downloaded")
		}
	})
}

// TestQuickBlockFetching tests the quick block fetching feature
func TestQuickBlockFetching(t *testing.T) {
	// Setup test environment
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stdout, log.LevelInfo, true)))

	// Create mock block retriever
	blockRetriever := newMockBlockRetriever()
	headerRequester := newMockHeaderRequester(50 * time.Millisecond)
	bodyRequester := newMockBodyRequester(50 * time.Millisecond)

	// Create blockchain
	parent := types.NewBlock(&types.Header{
		Number:     big.NewInt(10),
		ParentHash: common.Hash{},
	}, nil, nil, nil)
	blockRetriever.blocks[parent.Hash()] = parent

	// Generate child block
	block := types.NewBlock(&types.Header{
		Number:     big.NewInt(11),
		ParentHash: parent.Hash(),
	}, nil, nil, nil)

	// Prepare quick fetching response
	var fetchRangeBlocksCalled atomic.Bool
	var fetchRangeBlocksHash common.Hash
	var fetchRangeBlocksNumber uint64

	// Create fetcher with quick block fetching support
	fetcher := NewBlockFetcher(
		blockRetriever.getBlock,
		func(header *types.Header) error { return nil },
		func(block *types.Block, propagate bool) {},
		func() uint64 { return 10 }, // Current height
		func() uint64 { return 5 },  // Finalized height
		func(blocks types.Blocks) (int, error) {
			// Add blocks to local blockchain
			for _, block := range blocks {
				blockRetriever.blocks[block.Hash()] = block
			}
			return len(blocks), nil
		},
		func(id string) {},
		// fetchRangeBlocks function simulates quick block fetching
		func(peer string, startHeight uint64, startHash common.Hash, count uint64) ([]*types.Block, error) {
			fetchRangeBlocksCalled.Store(true)
			fetchRangeBlocksHash = startHash
			fetchRangeBlocksNumber = startHeight

			// Return requested block
			return []*types.Block{block}, nil
		},
	)

	// Start fetcher
	fetcher.Start()
	defer fetcher.Stop()

	// Send block notification
	err := fetcher.Notify("peer1", block.Hash(), block.NumberU64(), time.Now(),
		headerRequester.requestHeader, bodyRequester.requestBodies)
	if err != nil {
		t.Fatalf("Notify failed: %v", err)
	}

	// Wait for block to be fetched via quick path
	time.Sleep(200 * time.Millisecond)

	// Verify if fetchRangeBlocks was called
	if !fetchRangeBlocksCalled.Load() {
		t.Error("fetchRangeBlocks was not called")
	}

	// Verify if fetchRangeBlocks parameters are correct
	if fetchRangeBlocksHash != block.Hash() {
		t.Errorf("Expected hash %s, got %s", block.Hash().String(), fetchRangeBlocksHash.String())
	}
	if fetchRangeBlocksNumber != block.NumberU64() {
		t.Errorf("Expected number %d, got %d", block.NumberU64(), fetchRangeBlocksNumber)
	}

	// Verify if block was imported correctly
	if fetchedBlock := blockRetriever.getBlock(block.Hash()); fetchedBlock == nil {
		t.Error("Block was not imported through quick block fetching")
	}
}
