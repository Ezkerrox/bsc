// submit the evidence of malicious voting
package main

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/Ezkerrox/bsc/accounts/abi/bind"
	"github.com/Ezkerrox/bsc/common"
	"github.com/Ezkerrox/bsc/core/systemcontracts"
	"github.com/Ezkerrox/bsc/core/types"
	"github.com/Ezkerrox/bsc/crypto"
	"github.com/Ezkerrox/bsc/ethclient"
	"github.com/Ezkerrox/bsc/internal/flags"
	"github.com/Ezkerrox/bsc/log"
	"github.com/urfave/cli/v2"
)

var (
	app *cli.App

	senderFlag = &cli.StringFlag{
		Name:  "sender",
		Usage: "raw private key in hex format without 0x prefix; check permission on your own",
	}
	nodeFlag = &cli.StringFlag{
		Name:  "node",
		Usage: "rpc endpoint, http,https,ws,wss,ipc are supported",
	}
	chainIdFlag = &cli.UintFlag{
		Name:  "chainId",
		Usage: "chainId, can get by eth_chainId",
	}
	evidenceFlag = &cli.StringFlag{
		Name:  "evidence",
		Usage: "params for submitFinalityViolationEvidence in json format; string",
	}
)

func init() {
	app = flags.NewApp("a tool for submitting the evidence of malicious voting")
	app.Name = "maliciousvote-submit"
	app.Flags = []cli.Flag{
		senderFlag,
		nodeFlag,
		chainIdFlag,
		evidenceFlag,
	}
	app.Action = submitMaliciousVotes
}

func submitMaliciousVotes(c *cli.Context) error {
	// get sender
	senderRawKey := c.String(senderFlag.Name)
	if senderRawKey == "" {
		log.Crit("no sender specified (--sender)")
	}
	sender, err := crypto.HexToECDSA(senderRawKey)
	if err != nil {
		log.Crit("get sender failed", "error", err)
	} else {
		log.Info("get sender success")
	}

	// connect to the given URL
	nodeURL := c.String(nodeFlag.Name)
	if nodeURL == "" {
		log.Crit("no node specified (--node)")
	}
	client, err := ethclient.Dial(nodeURL)
	if err != nil {
		log.Crit("Error connecting to client", "nodeURL", nodeURL, "error", err)
	} else {
		// when nodeURL is type of http or https, err==nil not mean successfully connected
		if !strings.HasPrefix(nodeURL, "http") {
			log.Info("Successfully connected to client", "nodeURL", nodeURL)
		}
	}

	// get chainId
	chainId := c.Uint(chainIdFlag.Name)
	if chainId == 0 {
		log.Crit("no chainId specified (--chainId)")
	} else {
		log.Info("get chainId success", "chainId", chainId)
	}

	// get evidence
	evidenceJson := c.String(evidenceFlag.Name)
	if evidenceJson == "" {
		log.Crit("no evidence specified (--evidence)")
	}
	var evidence SlashIndicatorFinalityEvidence
	if err = evidence.UnmarshalJSON([]byte(evidenceJson)); err != nil {
		log.Crit("Error parsing evidence", "error", err)
	} else {
		log.Info("get evidence success")
	}

	ops, _ := bind.NewKeyedTransactorWithChainID(sender, big.NewInt(int64(chainId)))
	//ops.GasLimit = 800000
	slashIndicator, _ := NewSlashIndicator(common.HexToAddress(systemcontracts.SlashContract), client)
	tx, err := slashIndicator.SubmitFinalityViolationEvidence(ops, evidence)
	if err != nil {
		log.Crit("submitMaliciousVotes:", "error", err)
	}
	var rc *types.Receipt
	for i := 0; i < 180; i++ {
		rc, err = client.TransactionReceipt(context.Background(), tx.Hash())
		if err == nil && rc.Status != 0 {
			log.Info("submitMaliciousVotes: submit evidence success", "receipt", rc)
			break
		}
		if rc != nil && rc.Status == 0 {
			log.Crit("submitMaliciousVotes: tx failed: ", "error", err, "receipt", rc)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if rc == nil {
		log.Crit("submitMaliciousVotes: submit evidence failed")
	}

	return nil
}

func main() {
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stderr, log.LevelInfo, true)))

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
