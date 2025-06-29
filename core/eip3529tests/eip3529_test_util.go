package eip3529tests

import (
	"math/big"
	"testing"

	"github.com/Ezkerrox/bsc/common"
	"github.com/Ezkerrox/bsc/consensus"
	"github.com/Ezkerrox/bsc/core"
	"github.com/Ezkerrox/bsc/core/rawdb"
	"github.com/Ezkerrox/bsc/core/types"
	"github.com/Ezkerrox/bsc/core/vm"
	"github.com/Ezkerrox/bsc/crypto"
	"github.com/Ezkerrox/bsc/params"
	"github.com/Ezkerrox/bsc/triedb"
)

func newGwei(n int64) *big.Int {
	return new(big.Int).Mul(big.NewInt(n), big.NewInt(params.GWei))
}

// Test the gas used by running a transaction sent to a smart contract with given bytecode and storage.
func TestGasUsage(t *testing.T, config *params.ChainConfig, engine consensus.Engine, bytecode []byte, initialStorage map[common.Hash]common.Hash, initialGas, expectedGasUsed uint64) {
	var (
		aa = common.HexToAddress("0x000000000000000000000000000000000000aaaa")

		// Generate a canonical chain to act as the main dataset
		db = rawdb.NewMemoryDatabase()

		// A sender who makes transactions, has some funds
		key, _        = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address       = crypto.PubkeyToAddress(key.PublicKey)
		balanceBefore = big.NewInt(1000000000000000)
		gspec         = &core.Genesis{
			Config: config,
			Alloc: types.GenesisAlloc{
				address: {Balance: balanceBefore},
				aa: {
					Code:    bytecode,
					Storage: initialStorage,
					Nonce:   0,
					Balance: big.NewInt(0),
				},
			},
		}
		genesis = gspec.MustCommit(db, triedb.NewDatabase(db, nil))
	)

	blocks, _ := core.GenerateChain(gspec.Config, genesis, engine, db, 1, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})

		// One transaction to 0xAAAA
		signer := types.LatestSigner(gspec.Config)
		tx, _ := types.SignNewTx(key, signer, &types.LegacyTx{
			Nonce:    0,
			To:       &aa,
			Gas:      initialGas,
			GasPrice: newGwei(5),
		})
		b.AddTx(tx)
	})

	// Import the canonical chain
	diskdb := rawdb.NewMemoryDatabase()
	gspec.MustCommit(diskdb, triedb.NewDatabase(diskdb, nil))

	chain, err := core.NewBlockChain(diskdb, nil, gspec, nil, engine, vm.Config{}, nil, nil)
	if err != nil {
		t.Fatalf("failed to create tester chain: %v", err)
	}
	if n, err := chain.InsertChain(blocks); err != nil {
		t.Fatalf("block %d: failed to insert into chain: %v", n, err)
	}

	block := chain.GetBlockByNumber(1)

	if block.GasUsed() != expectedGasUsed {
		t.Fatalf("incorrect amount of gas spent: expected %d, got %d", expectedGasUsed, block.GasUsed())
	}
}
