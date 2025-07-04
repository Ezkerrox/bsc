// Copyright 2020 The go-ethereum Authors
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

// Tests that abnormal program termination (i.e.crash) and restart doesn't leave
// the database in some strange state with gaps in the chain, nor with block data
// dangling in the future.

package core

import (
	"encoding/hex"
	"math/big"
	"testing"
	"time"

	"github.com/Ezkerrox/bsc/common"
	"github.com/Ezkerrox/bsc/consensus/ethash"
	"github.com/Ezkerrox/bsc/core/rawdb"
	"github.com/Ezkerrox/bsc/core/types"
	"github.com/Ezkerrox/bsc/core/vm"
	"github.com/Ezkerrox/bsc/crypto"
	"github.com/Ezkerrox/bsc/ethdb"
	"github.com/Ezkerrox/bsc/ethdb/memorydb"
	"github.com/Ezkerrox/bsc/params"
)

var (
	// testKey is a private key to use for funding a tester account.
	testKey, _       = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	contractCode, _  = hex.DecodeString("608060405260016000806101000a81548160ff02191690831515021790555034801561002a57600080fd5b506101688061003a6000396000f3fe608060405234801561001057600080fd5b50600436106100365760003560e01c806389a2d8011461003b578063b0483f4814610059575b600080fd5b610043610075565b60405161005091906100f4565b60405180910390f35b610073600480360381019061006e91906100bc565b61008b565b005b60008060009054906101000a900460ff16905090565b806000806101000a81548160ff02191690831515021790555050565b6000813590506100b68161011b565b92915050565b6000602082840312156100ce57600080fd5b60006100dc848285016100a7565b91505092915050565b6100ee8161010f565b82525050565b600060208201905061010960008301846100e5565b92915050565b60008115159050919050565b6101248161010f565b811461012f57600080fd5b5056fea264697066735822122092f788b569bfc3786e90601b5dbec01cfc3d76094164fd66ca7d599c4239fc5164736f6c63430008000033")
	contractAddr     = common.HexToAddress("0xe74a3c7427cda785e0000d42a705b1f3fd371e09")
	contractData1, _ = hex.DecodeString("b0483f480000000000000000000000000000000000000000000000000000000000000000")
	contractData2, _ = hex.DecodeString("b0483f480000000000000000000000000000000000000000000000000000000000000001")
	commonGas        = 192138
	// testAddr is the Ethereum address of the tester account.
	testAddr = crypto.PubkeyToAddress(testKey.PublicKey)

	// testBlocks is the test parameters array for specific blocks.
	testBlocks = []testBlockParam{
		{
			// This txs params also used to default block.
			blockNr: 11,
			txs: []testTransactionParam{
				{
					to:       &common.Address{0x01},
					value:    big.NewInt(1),
					gasPrice: big.NewInt(params.InitialBaseFee),
					data:     nil,
				},
			},
		},
		{
			blockNr: 12,
			txs: []testTransactionParam{
				{
					to:       &common.Address{0x01},
					value:    big.NewInt(1),
					gasPrice: big.NewInt(params.InitialBaseFee),
					data:     nil,
				},
				{
					to:       &common.Address{0x02},
					value:    big.NewInt(2),
					gasPrice: big.NewInt(params.InitialBaseFee + 1),
					data:     nil,
				},
				{
					to:       nil,
					value:    big.NewInt(0),
					gasPrice: big.NewInt(params.InitialBaseFee + 1),
					data:     contractCode,
				},
			},
		},
		{
			blockNr: 13,
			txs: []testTransactionParam{
				{
					to:       &common.Address{0x01},
					value:    big.NewInt(1),
					gasPrice: big.NewInt(params.InitialBaseFee),
					data:     nil,
				},
				{
					to:       &common.Address{0x02},
					value:    big.NewInt(2),
					gasPrice: big.NewInt(params.InitialBaseFee + 1),
					data:     nil,
				},
				{
					to:       &common.Address{0x03},
					value:    big.NewInt(3),
					gasPrice: big.NewInt(params.InitialBaseFee + 2),
					data:     nil,
				},
				{
					to:       &contractAddr,
					value:    big.NewInt(0),
					gasPrice: big.NewInt(params.InitialBaseFee + 2),
					data:     contractData1,
				},
			},
		},
		{
			blockNr: 14,
			txs: []testTransactionParam{
				{
					to:       &contractAddr,
					value:    big.NewInt(0),
					gasPrice: big.NewInt(params.InitialBaseFee + 2),
					data:     contractData2,
				},
			},
		},
		{
			blockNr: 15,
			txs:     []testTransactionParam{},
		},
	}
)

type testTransactionParam struct {
	to       *common.Address
	value    *big.Int
	gasPrice *big.Int
	data     []byte
}

type testBlockParam struct {
	blockNr int
	txs     []testTransactionParam
}

// testBackend is a mock implementation of the live Ethereum message handler. Its
// purpose is to allow testing the request/reply workflows and wire serialization
// in the `eth` protocol without actually doing any data processing.
type testBackend struct {
	db    ethdb.Database
	chain *BlockChain
}

// newTestBackend creates an empty chain and wraps it into a mock backend.
func newTestBackend(blocks int, light bool) *testBackend {
	return newTestBackendWithGenerator(blocks, light)
}

// newTestBackendWithGenerator creates a chain with a number of explicitly defined blocks and
// wraps it into a mock backend.
func newTestBackendWithGenerator(blocks int, lightProcess bool) *testBackend {
	signer := types.HomesteadSigner{}
	// Create a database pre-initialize with a genesis block
	db := rawdb.NewMemoryDatabase()
	db.SetDiffStore(memorydb.New())
	gspec := &Genesis{
		Config:  params.TestChainConfig,
		Alloc:   GenesisAlloc{testAddr: {Balance: big.NewInt(100000000000000000)}},
		BaseFee: big.NewInt(params.InitialBaseFee),
	}
	chain, _ := NewBlockChain(db, nil, gspec, nil, ethash.NewFaker(), vm.Config{}, nil, nil, EnablePersistDiff(860000))
	generator := func(i int, block *BlockGen) {
		// The chain maker doesn't have access to a chain, so the difficulty will be
		// lets unset (nil). Set it here to the correct value.
		block.SetCoinbase(testAddr)

		for idx, testBlock := range testBlocks {
			// Specific block setting, the index in this generator has 1 diff from specified blockNr.
			if i+1 == testBlock.blockNr {
				for _, testTransaction := range testBlock.txs {
					var transaction *types.Transaction
					if testTransaction.to == nil {
						transaction = types.NewContractCreation(block.TxNonce(testAddr),
							testTransaction.value, uint64(commonGas), testTransaction.gasPrice, testTransaction.data)
					} else {
						transaction = types.NewTransaction(block.TxNonce(testAddr), *testTransaction.to,
							testTransaction.value, uint64(commonGas), testTransaction.gasPrice, testTransaction.data)
					}
					tx, err := types.SignTx(transaction, signer, testKey)
					if err != nil {
						panic(err)
					}
					block.AddTxWithChain(chain, tx)
				}
				break
			}

			// Default block setting.
			if idx == len(testBlocks)-1 {
				// We want to simulate an empty middle block, having the same state as the
				// first one. The last is needs a state change again to force a reorg.
				for _, testTransaction := range testBlocks[0].txs {
					tx, err := types.SignTx(types.NewTransaction(block.TxNonce(testAddr), *testTransaction.to,
						testTransaction.value, uint64(commonGas), testTransaction.gasPrice, testTransaction.data), signer, testKey)
					if err != nil {
						panic(err)
					}
					block.AddTxWithChain(chain, tx)
				}
			}
		}
	}
	bs, _ := GenerateChain(params.TestChainConfig, chain.Genesis(), ethash.NewFaker(), db, blocks, generator)
	if _, err := chain.InsertChain(bs); err != nil {
		panic(err)
	}

	return &testBackend{
		db:    db,
		chain: chain,
	}
}

// close tears down the transaction pool and chain behind the mock backend.
func (b *testBackend) close() {
	b.chain.Stop()
}

func (b *testBackend) Chain() *BlockChain { return b.chain }

func TestFreezeDiffLayer(t *testing.T) {
	blockNum := 1024
	fullBackend := newTestBackend(blockNum, true)
	defer fullBackend.close()
	for len(fullBackend.chain.diffQueueBuffer) > 0 {
		// Wait for the buffer to be zero.
	}
	// Minus one empty block.
	if fullBackend.chain.diffQueue.Size() != blockNum-1 {
		t.Errorf("size of diff queue is wrong, expected: %d, get: %d", blockNum-1, fullBackend.chain.diffQueue.Size())
	}

	time.Sleep(diffLayerFreezerRecheckInterval + 2*time.Second)
	if fullBackend.chain.diffQueue.Size() != int(fullBackend.chain.triesInMemory) {
		t.Errorf("size of diff queue is wrong, expected: %d, get: %d", blockNum, fullBackend.chain.diffQueue.Size())
	}

	block := fullBackend.chain.GetBlockByNumber(uint64(blockNum / 2))
	diffStore := fullBackend.chain.db.DiffStore()
	rawData := rawdb.ReadDiffLayerRLP(diffStore, block.Hash())
	if len(rawData) == 0 {
		t.Error("do not find diff layer in db")
	}
}

// newTwoForkedBlockchains returns two blockchains, these two chains are generated by different
// generators, they have some same parent blocks, the number of same blocks are determined by
// testBlocks, once chain1 inserted a non-default block, chain1 and chain2 get forked.
func newTwoForkedBlockchains(len1, len2 int) (chain1 *BlockChain, chain2 *BlockChain) {
	signer := types.HomesteadSigner{}
	// Create a database pre-initialize with a genesis block
	db1 := rawdb.NewMemoryDatabase()
	db1.SetDiffStore(memorydb.New())
	gspec := &Genesis{
		Config:  params.TestChainConfig,
		Alloc:   GenesisAlloc{testAddr: {Balance: big.NewInt(100000000000000000)}},
		BaseFee: big.NewInt(params.InitialBaseFee),
	}
	engine1 := ethash.NewFaker()
	chain1, _ = NewBlockChain(db1, nil, gspec, nil, engine1, vm.Config{}, nil, nil, EnablePersistDiff(860000), EnableBlockValidator(params.TestChainConfig, 0, nil))
	generator1 := func(i int, block *BlockGen) {
		// The chain maker doesn't have access to a chain, so the difficulty will be
		// lets unset (nil). Set it here to the correct value.
		block.SetCoinbase(testAddr)

		for idx, testBlock := range testBlocks {
			// Specific block setting, the index in this generator has 1 diff from specified blockNr.
			if i+1 == testBlock.blockNr {
				for _, testTransaction := range testBlock.txs {
					var transaction *types.Transaction
					if testTransaction.to == nil {
						transaction = types.NewContractCreation(block.TxNonce(testAddr),
							testTransaction.value, uint64(commonGas), testTransaction.gasPrice, testTransaction.data)
					} else {
						transaction = types.NewTransaction(block.TxNonce(testAddr), *testTransaction.to,
							testTransaction.value, uint64(commonGas), testTransaction.gasPrice, testTransaction.data)
					}
					tx, err := types.SignTx(transaction, signer, testKey)
					if err != nil {
						panic(err)
					}
					block.AddTxWithChain(chain1, tx)
				}
				break
			}

			// Default block setting.
			if idx == len(testBlocks)-1 {
				// We want to simulate an empty middle block, having the same state as the
				// first one. The last is needs a state change again to force a reorg.
				for _, testTransaction := range testBlocks[0].txs {
					tx, err := types.SignTx(types.NewTransaction(block.TxNonce(testAddr), *testTransaction.to,
						testTransaction.value, uint64(commonGas), testTransaction.gasPrice, testTransaction.data), signer, testKey)
					if err != nil {
						panic(err)
					}
					block.AddTxWithChain(chain1, tx)
				}
			}
		}
	}
	bs1, _ := GenerateChain(params.TestChainConfig, chain1.Genesis(), ethash.NewFaker(), db1, len1, generator1)
	if _, err := chain1.InsertChain(bs1); err != nil {
		panic(err)
	}
	waitDifflayerCached(chain1, bs1)

	// Create a database pre-initialize with a genesis block
	db2 := rawdb.NewMemoryDatabase()
	db2.SetDiffStore(memorydb.New())
	gspec2 := &Genesis{
		Config:  params.TestChainConfig,
		Alloc:   GenesisAlloc{testAddr: {Balance: big.NewInt(100000000000000000)}},
		BaseFee: big.NewInt(params.InitialBaseFee),
	}
	chain2, _ = NewBlockChain(db2, nil, gspec2, nil, ethash.NewFaker(), vm.Config{}, nil, nil, EnablePersistDiff(860000), EnableBlockValidator(params.TestChainConfig, 0, nil))
	generator2 := func(i int, block *BlockGen) {
		// The chain maker doesn't have access to a chain, so the difficulty will be
		// lets unset (nil). Set it here to the correct value.
		block.SetCoinbase(testAddr)
		// We want to simulate an empty middle block, having the same state as the
		// first one. The last is needs a state change again to force a reorg.
		for _, testTransaction := range testBlocks[0].txs {
			tx, err := types.SignTx(types.NewTransaction(block.TxNonce(testAddr), *testTransaction.to,
				testTransaction.value, uint64(commonGas), testTransaction.gasPrice, testTransaction.data), signer, testKey)
			if err != nil {
				panic(err)
			}
			block.AddTxWithChain(chain1, tx)
		}
	}
	bs2, _ := GenerateChain(params.TestChainConfig, chain2.Genesis(), ethash.NewFaker(), db2, len2, generator2)
	if _, err := chain2.InsertChain(bs2); err != nil {
		panic(err)
	}
	waitDifflayerCached(chain2, bs2)

	return chain1, chain2
}

func waitDifflayerCached(chain *BlockChain, bs types.Blocks) {
	for _, block := range bs {
		// wait for all difflayers to be cached
		for block.Header().TxHash != types.EmptyRootHash &&
			chain.GetTrustedDiffLayer(block.Hash()) == nil {
			time.Sleep(time.Second)
		}
	}
}

func testGetRootByDiffHash(t *testing.T, chain1, chain2 *BlockChain, blockNumber uint64, status types.VerifyStatus) {
	block2 := chain2.GetBlockByNumber(blockNumber)
	if block2 == nil {
		t.Fatalf("failed to find block, number: %v", blockNumber)
	}
	expect := VerifyResult{
		Status:      status,
		BlockNumber: blockNumber,
		BlockHash:   block2.Hash(),
	}
	if status.Code&0xff00 == types.StatusVerified.Code {
		expect.Root = block2.Root()
	}

	diffLayer2 := chain2.GetTrustedDiffLayer(block2.Hash())
	if diffLayer2 == nil {
		t.Fatal("failed to find diff layer")
	}
	diffHash2 := types.EmptyRootHash
	if status != types.StatusDiffHashMismatch {
		var err error
		diffHash2, err = CalculateDiffHash(diffLayer2)
		if err != nil {
			t.Fatalf("failed to compute diff hash: %v", err)
		}
	}

	if status == types.StatusPartiallyVerified {
		block1 := chain1.GetBlockByNumber(blockNumber)
		if block1 == nil {
			t.Fatalf("failed to find block, number: %v", blockNumber)
		}
		chain1.diffLayerCache.Remove(block1.Hash())
	}

	result := chain1.GetVerifyResult(blockNumber, block2.Hash(), diffHash2)
	if result.Status != expect.Status {
		t.Fatalf("failed to verify block, number: %v, expect status: %v, real status: %v", blockNumber, expect.Status, result.Status)
	}
	if result.Root != expect.Root {
		t.Fatalf("failed to verify block, number: %v, expect root: %v, real root: %v", blockNumber, expect.Root, result.Root)
	}
}

func TestGetRootByDiffHash(t *testing.T) {
	len1 := 23 // length of blockchain1
	len2 := 35 // length of blockchain2
	plen := 11 // length of same parent blocks, which determined by testBlocks.

	chain1, chain2 := newTwoForkedBlockchains(len1, len2)
	defer chain1.Stop()
	defer chain2.Stop()

	hash1 := chain1.GetBlockByNumber(uint64(plen)).Hash()
	hash2 := chain2.GetBlockByNumber(uint64(plen)).Hash()
	if hash1 != hash2 {
		t.Errorf("chain content mismatch at %d: have hash %v, want hash %v", plen, hash2, hash1)
	}

	testGetRootByDiffHash(t, chain1, chain2, 10, types.StatusFullVerified)
	testGetRootByDiffHash(t, chain1, chain2, 2, types.StatusPartiallyVerified)
	testGetRootByDiffHash(t, chain1, chain2, 10, types.StatusDiffHashMismatch)
	testGetRootByDiffHash(t, chain1, chain2, 12, types.StatusImpossibleFork)
	testGetRootByDiffHash(t, chain1, chain2, 20, types.StatusPossibleFork)
	testGetRootByDiffHash(t, chain1, chain2, 24, types.StatusBlockNewer)
	testGetRootByDiffHash(t, chain1, chain2, 35, types.StatusBlockTooNew)
}
