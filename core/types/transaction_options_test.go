package types

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/Ezkerrox/bsc/common"
	"github.com/Ezkerrox/bsc/common/hexutil"
)

func ptr(hash common.Hash) *common.Hash {
	return &hash
}

func u64Ptr(v hexutil.Uint64) *hexutil.Uint64 {
	return &v
}

func TestTransactionOptsJSONUnMarshalTrip(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		mustFail bool
		expected TransactionOpts
	}{
		{
			"StateRoot",
			`{"knownAccounts":{"0x6b3A8798E5Fb9fC5603F3aB5eA2e8136694e55d0":"0x290decd9548b62a8d60345a988386fc84ba6bc95484008f6362f93160ef3e563"}}`,
			false,
			TransactionOpts{
				KnownAccounts: map[common.Address]AccountStorage{
					common.HexToAddress("0x6b3A8798E5Fb9fC5603F3aB5eA2e8136694e55d0"): AccountStorage{
						StorageRoot: ptr(common.HexToHash("0x290decd9548b62a8d60345a988386fc84ba6bc95484008f6362f93160ef3e563")),
					},
				},
			},
		},
		{
			"StorageSlots",
			`{"knownAccounts":{"0x6b3A8798E5Fb9fC5603F3aB5eA2e8136694e55d0":{"0xc65a7bb8d6351c1cf70c95a316cc6a92839c986682d98bc35f958f4883f9d2a8":"0x0000000000000000000000000000000000000000000000000000000000000000"}}}`,
			false,
			TransactionOpts{
				KnownAccounts: map[common.Address]AccountStorage{
					common.HexToAddress("0x6b3A8798E5Fb9fC5603F3aB5eA2e8136694e55d0"): AccountStorage{
						StorageRoot: nil,
						StorageSlots: map[common.Hash]common.Hash{
							common.HexToHash("0xc65a7bb8d6351c1cf70c95a316cc6a92839c986682d98bc35f958f4883f9d2a8"): common.HexToHash("0x"),
						},
					},
				},
			},
		},
		{
			"EmptyObject",
			`{"knownAccounts":{}}`,
			false,
			TransactionOpts{
				KnownAccounts: make(map[common.Address]AccountStorage),
			},
		},
		{
			"EmptyStrings",
			`{"knownAccounts":{"":""}}`,
			true,
			TransactionOpts{
				KnownAccounts: nil,
			},
		},
		{
			"BlockNumberMin",
			`{"blockNumberMin":"0x1"}`,
			false,
			TransactionOpts{
				BlockNumberMin: u64Ptr(1),
			},
		},
		{
			"BlockNumberMax",
			`{"blockNumberMin":"0x1", "blockNumberMax":"0x2"}`,
			false,
			TransactionOpts{
				BlockNumberMin: u64Ptr(1),
				BlockNumberMax: u64Ptr(2),
			},
		},
		{
			"TimestampMin",
			`{"timestampMin":"0xffff"}`,
			false,
			TransactionOpts{
				TimestampMin: u64Ptr(0xffff),
			},
		},
		{
			"TimestampMax",
			`{"timestampMax":"0xffffff"}`,
			false,
			TransactionOpts{
				TimestampMax: u64Ptr(0xffffff),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var opts TransactionOpts
			err := json.Unmarshal([]byte(test.input), &opts)
			if test.mustFail && err == nil {
				t.Errorf("Test %s should fail", test.name)
			}
			if !test.mustFail && err != nil {
				t.Errorf("Test %s should pass but got err: %v", test.name, err)
			}

			if !reflect.DeepEqual(opts, test.expected) {
				t.Errorf("Test %s got unexpected value, want %#v, got %#v", test.name, test.expected, opts)
			}
		})
	}
}
