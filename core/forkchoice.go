// Copyright 2021 The go-ethereum Authors
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

package core

import (
	crand "crypto/rand"
	"errors"
	"math"
	"math/big"
	mrand "math/rand"

	"github.com/Ezkerrox/bsc/common"
	"github.com/Ezkerrox/bsc/consensus"
	"github.com/Ezkerrox/bsc/core/types"
	"github.com/Ezkerrox/bsc/log"
	"github.com/Ezkerrox/bsc/params"
)

// ChainReader defines a small collection of methods needed to access the local
// blockchain during header verification. It's implemented by blockchain.
type ChainReader interface {
	// Config retrieves the header chain's chain configuration.
	Config() *params.ChainConfig

	// Engine retrieves the blockchain's consensus engine.
	Engine() consensus.Engine

	// GetJustifiedNumber returns the highest justified blockNumber on the branch including and before `header`
	GetJustifiedNumber(header *types.Header) uint64

	// GetTd returns the total difficulty of a local block.
	GetTd(common.Hash, uint64) *big.Int
}

// ForkChoice is the fork chooser based on the highest total difficulty of the
// chain(the fork choice used in the eth1) and the external fork choice (the fork
// choice used in the eth2). This main goal of this ForkChoice is not only for
// offering fork choice during the eth1/2 merge phase, but also keep the compatibility
// for all other proof-of-work networks.
type ForkChoice struct {
	chain ChainReader
	rand  *mrand.Rand

	// preserve is a helper function used in td fork choice.
	// Miners will prefer to choose the local mined block if the
	// local td is equal to the extern one. It can be nil for light
	// client
	preserve func(header *types.Header) bool
}

func NewForkChoice(chainReader ChainReader, preserve func(header *types.Header) bool) *ForkChoice {
	// Seed a fast but crypto originating random generator
	seed, err := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		log.Crit("Failed to initialize random seed", "err", err)
	}
	return &ForkChoice{
		chain:    chainReader,
		rand:     mrand.New(mrand.NewSource(seed.Int64())),
		preserve: preserve,
	}
}

// ReorgNeeded returns whether the reorg should be applied
// based on the given external header and local canonical chain.
// In the td mode, the new head is chosen if the corresponding
// total difficulty is higher. In the extern mode, the trusted
// header is always selected as the head.
func (f *ForkChoice) ReorgNeeded(current *types.Header, extern *types.Header) (bool, error) {
	var (
		localTD  = f.chain.GetTd(current.Hash(), current.Number.Uint64())
		externTd = f.chain.GetTd(extern.Hash(), extern.Number.Uint64())
	)
	if localTD == nil {
		return false, errors.New("missing td")
	}
	if externTd == nil {
		ptd := f.chain.GetTd(extern.ParentHash, extern.Number.Uint64()-1)
		if ptd == nil {
			return false, consensus.ErrUnknownAncestor
		}
		externTd = new(big.Int).Add(ptd, extern.Difficulty)
	}
	// Accept the new header as the chain head if the transition
	// is already triggered. We assume all the headers after the
	// transition come from the trusted consensus layer.
	if ttd := f.chain.Config().TerminalTotalDifficulty; ttd != nil && ttd.Cmp(externTd) <= 0 {
		return true, nil
	}

	// If the total difficulty is higher than our known, add it to the canonical chain
	if diff := externTd.Cmp(localTD); diff > 0 {
		return true, nil
	} else if diff < 0 {
		return false, nil
	}
	// Local and external difficulty is identical.
	// Second clause in the if statement reduces the vulnerability to selfish mining.
	// Please refer to http://www.cs.cornell.edu/~ie53/publications/btcProcFC.pdf
	reorg := false
	externNum, localNum := extern.Number.Uint64(), current.Number.Uint64()
	if externNum < localNum {
		reorg = true
	} else if externNum == localNum {
		var currentPreserve, externPreserve bool
		if f.preserve != nil {
			currentPreserve, externPreserve = f.preserve(current), f.preserve(extern)
		}
		choiceRules := func() bool {
			if extern.Time == current.Time {
				doubleSign := (extern.Coinbase == current.Coinbase)
				if doubleSign {
					return extern.Hash().Cmp(current.Hash()) < 0
				} else {
					return f.rand.Float64() < 0.5
				}
			} else {
				return extern.Time < current.Time
			}
		}
		reorg = !currentPreserve && (externPreserve || choiceRules())
	}
	return reorg, nil
}

// ReorgNeededWithFastFinality compares justified block numbers firstly, backoff to compare tds when equal
func (f *ForkChoice) ReorgNeededWithFastFinality(current *types.Header, header *types.Header) (bool, error) {
	_, ok := f.chain.Engine().(consensus.PoSA)
	if !ok {
		return f.ReorgNeeded(current, header)
	}

	justifiedNumber, curJustifiedNumber := uint64(0), uint64(0)
	if f.chain.Config().IsPlato(header.Number) {
		justifiedNumber = f.chain.GetJustifiedNumber(header)
	}
	if f.chain.Config().IsPlato(current.Number) {
		curJustifiedNumber = f.chain.GetJustifiedNumber(current)
	}
	if justifiedNumber == curJustifiedNumber {
		return f.ReorgNeeded(current, header)
	}

	if justifiedNumber > curJustifiedNumber && header.Number.Cmp(current.Number) <= 0 {
		log.Info("Chain find higher justifiedNumber", "fromHeight", current.Number, "fromHash", current.Hash(), "fromMiner", current.Coinbase, "fromJustified", curJustifiedNumber,
			"toHeight", header.Number, "toHash", header.Hash(), "toMiner", header.Coinbase, "toJustified", justifiedNumber)
	}
	return justifiedNumber > curJustifiedNumber, nil
}
