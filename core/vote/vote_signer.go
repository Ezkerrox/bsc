package vote

import (
	"context"
	"os"
	"time"

	"github.com/pkg/errors"

	"github.com/prysmaticlabs/prysm/v5/crypto/bls"
	validatorpb "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1/validator-client"
	"github.com/prysmaticlabs/prysm/v5/validator/accounts/iface"
	"github.com/prysmaticlabs/prysm/v5/validator/accounts/wallet"
	"github.com/prysmaticlabs/prysm/v5/validator/keymanager"

	"github.com/Ezkerrox/bsc/core/types"
	"github.com/Ezkerrox/bsc/log"
	"github.com/Ezkerrox/bsc/metrics"
)

const (
	voteSignerTimeout = time.Second * 5
)

var votesSigningErrorCounter = metrics.NewRegisteredCounter("votesSigner/error", nil)

type VoteSigner struct {
	km     *keymanager.IKeymanager
	PubKey [48]byte
}

func NewVoteSigner(blsPasswordPath, blsWalletPath string) (*VoteSigner, error) {
	dirExists, err := wallet.Exists(blsWalletPath)
	if err != nil {
		log.Error("Check BLS wallet exists", "err", err)
		return nil, err
	}
	if !dirExists {
		log.Error("BLS wallet did not exists.")
		return nil, errors.New("BLS wallet did not exists")
	}

	walletPassword, err := os.ReadFile(blsPasswordPath)
	if err != nil {
		log.Error("Read BLS wallet password", "err", err)
		return nil, err
	}
	log.Info("Read BLS wallet password successfully")

	w, err := wallet.OpenWallet(context.Background(), &wallet.Config{
		WalletDir:      blsWalletPath,
		WalletPassword: string(walletPassword),
	})
	if err != nil {
		log.Error("Open BLS wallet failed", "err", err)
		return nil, err
	}
	log.Info("Open BLS wallet successfully")

	km, err := w.InitializeKeymanager(context.Background(), iface.InitKeymanagerConfig{ListenForChanges: false})
	if err != nil {
		log.Error("Initialize key manager failed", "err", err)
		return nil, err
	}
	log.Info("Initialized keymanager successfully")

	ctx, cancel := context.WithTimeout(context.Background(), voteSignerTimeout)
	defer cancel()

	pubKeys, err := km.FetchValidatingPublicKeys(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not fetch validating public keys")
	}

	return &VoteSigner{
		km:     &km,
		PubKey: pubKeys[0],
	}, nil
}

func (signer *VoteSigner) SignVote(vote *types.VoteEnvelope) error {
	// Sign the vote, fetch the first pubKey as validator's bls public key.
	pubKey := signer.PubKey
	blsPubKey, err := bls.PublicKeyFromBytes(pubKey[:])
	if err != nil {
		return errors.Wrap(err, "convert public key from bytes to bls failed")
	}

	voteDataHash := vote.Data.Hash()

	ctx, cancel := context.WithTimeout(context.Background(), voteSignerTimeout)
	defer cancel()

	signature, err := (*signer.km).Sign(ctx, &validatorpb.SignRequest{
		PublicKey:   pubKey[:],
		SigningRoot: voteDataHash[:],
	})
	if err != nil {
		return err
	}

	copy(vote.VoteAddress[:], blsPubKey.Marshal()[:])
	copy(vote.Signature[:], signature.Marshal()[:])
	return nil
}
