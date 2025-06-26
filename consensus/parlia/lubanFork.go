package parlia

import (
	"context"
	"math"
	"math/big"

	"github.com/Ezkerrox/bsc/common"
	"github.com/Ezkerrox/bsc/common/hexutil"
	"github.com/Ezkerrox/bsc/core/systemcontracts"
	"github.com/Ezkerrox/bsc/internal/ethapi"
	"github.com/Ezkerrox/bsc/log"
	"github.com/Ezkerrox/bsc/rpc"
)

func (p *Parlia) getCurrentValidatorsBeforeLuban(blockHash common.Hash, blockNumber *big.Int) ([]common.Address, error) {
	blockNr := rpc.BlockNumberOrHashWithHash(blockHash, false)

	// prepare different method
	method := "getValidators"
	if p.chainConfig.IsEuler(blockNumber) {
		method = "getMiningValidators"
	}

	ctx, cancel := context.WithCancel(context.Background())
	// cancel when we are finished consuming integers
	defer cancel()
	data, err := p.validatorSetABIBeforeLuban.Pack(method)
	if err != nil {
		log.Error("Unable to pack tx for getValidators", "error", err)
		return nil, err
	}
	// do smart contract call
	msgData := (hexutil.Bytes)(data)
	toAddress := common.HexToAddress(systemcontracts.ValidatorContract)
	gas := (hexutil.Uint64)(uint64(math.MaxUint64 / 2))
	result, err := p.ethAPI.Call(ctx, ethapi.TransactionArgs{
		Gas:  &gas,
		To:   &toAddress,
		Data: &msgData,
	}, &blockNr, nil, nil)
	if err != nil {
		return nil, err
	}

	var valSet []common.Address
	err = p.validatorSetABIBeforeLuban.UnpackIntoInterface(&valSet, method, result)
	return valSet, err
}
