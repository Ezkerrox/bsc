// Copyright 2017 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

// faucet is an Ether faucet backed by a light client.
package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"io"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Ezkerrox/bsc/accounts"
	"github.com/Ezkerrox/bsc/accounts/abi"
	"github.com/Ezkerrox/bsc/accounts/keystore"
	"github.com/Ezkerrox/bsc/common"
	"github.com/Ezkerrox/bsc/core"
	"github.com/Ezkerrox/bsc/core/types"
	"github.com/Ezkerrox/bsc/ethclient"
	"github.com/Ezkerrox/bsc/log"
	"github.com/Ezkerrox/bsc/params"
	"github.com/gorilla/websocket"
	"golang.org/x/time/rate"
)

var (
	genesisFlag       = flag.String("genesis", "", "Genesis json file to seed the chain with")
	apiPortFlag       = flag.Int("apiport", 8080, "Listener port for the HTTP API connection")
	wsEndpoint        = flag.String("ws", "http://127.0.0.1:7777/", "Url to ws endpoint")
	wsEndpointMainnet = flag.String("ws.mainnet", "", "Url to ws endpoint of BSC mainnet")

	netnameFlag = flag.String("faucet.name", "", "Network name to assign to the faucet")
	payoutFlag  = flag.Int("faucet.amount", 1, "Number of Ethers to pay out per user request")
	minutesFlag = flag.Int("faucet.minutes", 1440, "Number of minutes to wait between funding rounds")
	tiersFlag   = flag.Int("faucet.tiers", 3, "Number of funding tiers to enable (x3 time, x2.5 funds)")

	accJSONFlag = flag.String("account.json", "", "Key json file to fund user requests with")
	accPassFlag = flag.String("account.pass", "", "Decryption password to access faucet funds")

	captchaToken  = flag.String("captcha.token", "", "Recaptcha site key to authenticate client side")
	captchaSecret = flag.String("captcha.secret", "", "Recaptcha secret key to authenticate server side")

	noauthFlag = flag.Bool("noauth", false, "Enables funding requests without authentication")
	logFlag    = flag.Int("loglevel", 3, "Log level to use for Ethereum and the faucet")

	bep2eContracts     = flag.String("bep2eContracts", "", "the list of bep2p contracts")
	bep2eSymbols       = flag.String("bep2eSymbols", "", "the symbol of bep2p tokens")
	bep2eAmounts       = flag.String("bep2eAmounts", "", "the amount of bep2p tokens")
	fixGasPrice        = flag.Int64("faucet.fixedprice", 0, "Will use fixed gas price if specified")
	twitterTokenFlag   = flag.String("twitter.token", "", "Bearer token to authenticate with the v2 Twitter API")
	twitterTokenV1Flag = flag.String("twitter.token.v1", "", "Bearer token to authenticate with the v1.1 Twitter API")

	resendInterval    = 15 * time.Second
	resendBatchSize   = 3
	resendMaxGasPrice = big.NewInt(50 * params.GWei)
	wsReadTimeout     = 5 * time.Minute
	minMainnetBalance = big.NewInt(2 * 1e6 * params.GWei) // 0.002 bnb
)

var (
	ether        = new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	bep2eAbiJson = `[ { "anonymous": false, "inputs": [ { "indexed": true, "internalType": "address", "name": "owner", "type": "address" }, { "indexed": true, "internalType": "address", "name": "spender", "type": "address" }, { "indexed": false, "internalType": "uint256", "name": "value", "type": "uint256" } ], "name": "Approval", "type": "event" }, { "anonymous": false, "inputs": [ { "indexed": true, "internalType": "address", "name": "from", "type": "address" }, { "indexed": true, "internalType": "address", "name": "to", "type": "address" }, { "indexed": false, "internalType": "uint256", "name": "value", "type": "uint256" } ], "name": "Transfer", "type": "event" }, { "inputs": [], "name": "totalSupply", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "decimals", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "symbol", "outputs": [ { "internalType": "string", "name": "", "type": "string" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "getOwner", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "account", "type": "address" } ], "name": "balanceOf", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "recipient", "type": "address" }, { "internalType": "uint256", "name": "amount", "type": "uint256" } ], "name": "transfer", "outputs": [ { "internalType": "bool", "name": "", "type": "bool" } ], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_owner", "type": "address" }, { "internalType": "address", "name": "spender", "type": "address" } ], "name": "allowance", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "spender", "type": "address" }, { "internalType": "uint256", "name": "amount", "type": "uint256" } ], "name": "approve", "outputs": [ { "internalType": "bool", "name": "", "type": "bool" } ], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "sender", "type": "address" }, { "internalType": "address", "name": "recipient", "type": "address" }, { "internalType": "uint256", "name": "amount", "type": "uint256" } ], "name": "transferFrom", "outputs": [ { "internalType": "bool", "name": "", "type": "bool" } ], "stateMutability": "nonpayable", "type": "function" } ]`
)

//go:embed faucet.html
var websiteTmpl string

func weiToEtherStringFx(wei *big.Int, prec int) string {
	etherValue := new(big.Float).Quo(new(big.Float).SetInt(wei), big.NewFloat(params.Ether))
	// Format the big.Float directly to a string with the specified precision
	return etherValue.Text('f', prec)
}

func main() {
	// Parse the flags and set up the logger to print everything requested
	flag.Parse()
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stderr, log.FromLegacyLevel(*logFlag), false)))
	log.Info("faucet started")
	// Construct the payout tiers
	amounts := make([]string, *tiersFlag)
	for i := 0; i < *tiersFlag; i++ {
		// Calculate the amount for the next tier and format it
		amount := float64(*payoutFlag) * math.Pow(2.5, float64(i))
		amounts[i] = fmt.Sprintf("0.%s BNBs", strconv.FormatFloat(amount, 'f', -1, 64))
		if amount == 1 {
			amounts[i] = strings.TrimSuffix(amounts[i], "s")
		}
	}
	bep2eNumAmounts := make([]string, 0)
	if bep2eAmounts != nil && len(*bep2eAmounts) > 0 {
		bep2eNumAmounts = strings.Split(*bep2eAmounts, ",")
	}

	symbols := make([]string, 0)
	if bep2eSymbols != nil && len(*bep2eSymbols) > 0 {
		symbols = strings.Split(*bep2eSymbols, ",")
	}

	contracts := make([]string, 0)
	if bep2eContracts != nil && len(*bep2eContracts) > 0 {
		contracts = strings.Split(*bep2eContracts, ",")
	}

	if len(bep2eNumAmounts) != len(symbols) || len(symbols) != len(contracts) {
		log.Crit("Length of bep2eContracts, bep2eSymbols, bep2eAmounts mismatch")
	}

	bep2eInfos := make(map[string]bep2eInfo, len(symbols))
	for idx, s := range symbols {
		n, ok := big.NewInt(0).SetString(bep2eNumAmounts[idx], 10)
		if !ok {
			log.Crit("failed to parse bep2eAmounts")
		}
		amountStr := big.NewFloat(0).Quo(big.NewFloat(0).SetInt(n), big.NewFloat(0).SetInt64(params.Ether)).String()

		bep2eInfos[s] = bep2eInfo{
			Contract:  common.HexToAddress(contracts[idx]),
			Amount:    *n,
			AmountStr: amountStr,
		}
	}
	website := new(bytes.Buffer)
	err := template.Must(template.New("").Parse(websiteTmpl)).Execute(website, map[string]interface{}{
		"Network":    *netnameFlag,
		"Amounts":    amounts,
		"Recaptcha":  *captchaToken,
		"NoAuth":     *noauthFlag,
		"Bep2eInfos": bep2eInfos,
	})
	if err != nil {
		log.Crit("Failed to render the faucet template", "err", err)
	}
	// Load and parse the genesis block requested by the user
	genesis, err := getGenesis(*genesisFlag, false, false)
	if err != nil {
		log.Crit("Failed to read genesis block contents", "genesis", *genesisFlag, "err", err)
	}
	// Load up the account key and decrypt its password
	blob, err := os.ReadFile(*accPassFlag)
	if err != nil {
		log.Crit("Failed to read account password contents", "file", *accPassFlag, "err", err)
	}
	pass := strings.TrimSuffix(string(blob), "\n")

	ks := keystore.NewKeyStore(filepath.Join(os.Getenv("HOME"), ".faucet", "keys_2"), keystore.StandardScryptN, keystore.StandardScryptP)
	if blob, err = os.ReadFile(*accJSONFlag); err != nil {
		log.Crit("Failed to read account key contents", "file", *accJSONFlag, "err", err)
	}
	acc, err := ks.Import(blob, pass, pass)
	if err != nil && err != keystore.ErrAccountAlreadyExists {
		log.Crit("Failed to import faucet signer account", "err", err)
	}
	if err := ks.Unlock(acc, pass); err != nil {
		log.Crit("Failed to unlock faucet signer account", "err", err)
	}
	// Assemble and start the faucet light service
	faucet, err := newFaucet(genesis, *wsEndpoint, *wsEndpointMainnet, ks, website.Bytes(), bep2eInfos)
	if err != nil {
		log.Crit("Failed to start faucet", "err", err)
	}
	defer faucet.close()

	if err := faucet.listenAndServe(*apiPortFlag); err != nil {
		log.Crit("Failed to launch faucet API", "err", err)
	}
}

// request represents an accepted funding request.
type request struct {
	Avatar  string             `json:"avatar"`  // Avatar URL to make the UI nicer
	Account common.Address     `json:"account"` // Ethereum address being funded
	Time    time.Time          `json:"time"`    // Timestamp when the request was accepted
	Tx      *types.Transaction `json:"tx"`      // Transaction funding the account
}

type bep2eInfo struct {
	Contract  common.Address
	Amount    big.Int
	AmountStr string
}

// faucet represents a crypto faucet backed by an Ethereum light client.
type faucet struct {
	config        *params.ChainConfig // Chain configurations for signing
	client        *ethclient.Client   // Client connection to the Ethereum chain
	clientMainnet *ethclient.Client   // Client connection to BSC mainnet for balance check
	index         []byte              // Index page to serve up on the web

	keystore *keystore.KeyStore // Keystore containing the single signer
	account  accounts.Account   // Account funding user faucet requests
	head     *types.Header      // Current head header of the faucet
	balance  *big.Int           // Current balance of the faucet
	nonce    uint64             // Current pending nonce of the faucet
	price    *big.Int           // Current gas price to issue funds with

	conns    []*wsConn            // Currently live websocket connections
	timeouts map[string]time.Time // History of users and their funding timeouts
	reqs     []*request           // Currently pending funding requests
	update   chan struct{}        // Channel to signal request updates

	lock sync.RWMutex // Lock protecting the faucet's internals

	bep2eInfos map[string]bep2eInfo
	bep2eAbi   abi.ABI

	limiter *IPRateLimiter
}

// wsConn wraps a websocket connection with a write mutex as the underlying
// websocket library does not synchronize access to the stream.
type wsConn struct {
	conn  *websocket.Conn
	wlock sync.Mutex
}

func newFaucet(genesis *core.Genesis, url string, mainnetUrl string, ks *keystore.KeyStore, index []byte, bep2eInfos map[string]bep2eInfo) (*faucet, error) {
	bep2eAbi, err := abi.JSON(strings.NewReader(bep2eAbiJson))
	if err != nil {
		return nil, err
	}
	client, err := ethclient.Dial(url)
	if err != nil {
		return nil, err
	}
	clientMainnet, err := ethclient.Dial(mainnetUrl)
	if err != nil {
		// skip mainnet balance check if it there is no available mainnet endpoint
		log.Warn("dail mainnet endpoint failed", "mainnetUrl", mainnetUrl, "err", err)
	}

	// Allow 1 request per minute with burst of 5, and cache up to 1000 IPs
	limiter, err := NewIPRateLimiter(rate.Limit(1.0), 5, 1000)
	if err != nil {
		return nil, err
	}

	return &faucet{
		config:        genesis.Config,
		client:        client,
		clientMainnet: clientMainnet,
		index:         index,
		keystore:      ks,
		account:       ks.Accounts()[0],
		timeouts:      make(map[string]time.Time),
		update:        make(chan struct{}, 1),
		bep2eInfos:    bep2eInfos,
		bep2eAbi:      bep2eAbi,
		limiter:       limiter,
	}, nil
}

// close terminates the Ethereum connection and tears down the faucet.
func (f *faucet) close() {
	f.client.Close()
}

// listenAndServe registers the HTTP handlers for the faucet and boots it up
// for service user funding requests.
func (f *faucet) listenAndServe(port int) error {
	go f.loop()

	http.HandleFunc("/", f.webHandler)
	http.HandleFunc("/api", f.apiHandler)
	http.HandleFunc("/faucet-smart/api", f.apiHandler)
	return http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}

// webHandler handles all non-api requests, simply flattening and returning the
// faucet website.
func (f *faucet) webHandler(w http.ResponseWriter, r *http.Request) {
	w.Write(f.index)
}

// apiHandler handles requests for Ether grants and transaction statuses.
func (f *faucet) apiHandler(w http.ResponseWriter, r *http.Request) {
	ip := r.RemoteAddr
	if len(r.Header.Get("X-Forwarded-For")) > 0 {
		ips := strings.Split(r.Header.Get("X-Forwarded-For"), ",")
		if len(ips) > 0 {
			ip = strings.TrimSpace(ips[len(ips)-1])
		}
	}

	if !f.limiter.GetLimiter(ip).Allow() {
		log.Warn("Too many requests from client: ", "client", ip)
		http.Error(w, "Too many requests", http.StatusTooManyRequests)
		return
	}

	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	// Start tracking the connection and drop at the end
	defer conn.Close()
	ipsStr := r.Header.Get("X-Forwarded-For")
	ips := strings.Split(ipsStr, ",")
	if len(ips) < 2 {
		return
	}

	f.lock.Lock()
	wsconn := &wsConn{conn: conn}
	f.conns = append(f.conns, wsconn)
	f.lock.Unlock()

	defer func() {
		f.lock.Lock()
		for i, c := range f.conns {
			if c.conn == conn {
				f.conns = append(f.conns[:i], f.conns[i+1:]...)
				break
			}
		}
		f.lock.Unlock()
	}()
	// Gather the initial stats from the network to report
	var (
		head    *types.Header
		balance *big.Int
		nonce   uint64
	)
	for head == nil || balance == nil {
		// Retrieve the current stats cached by the faucet
		f.lock.RLock()
		if f.head != nil {
			head = types.CopyHeader(f.head)
		}
		if f.balance != nil {
			balance = new(big.Int).Set(f.balance)
		}
		nonce = f.nonce
		f.lock.RUnlock()

		if head == nil || balance == nil {
			// Report the faucet offline until initial stats are ready
			//lint:ignore ST1005 This error is to be displayed in the browser
			if err = sendError(wsconn, errors.New("Faucet offline")); err != nil {
				log.Warn("Failed to send faucet error to client", "err", err)
				return
			}
			time.Sleep(3 * time.Second)
		}
	}
	// Send over the initial stats and the latest header
	f.lock.RLock()
	reqs := f.reqs
	f.lock.RUnlock()
	if err = send(wsconn, map[string]interface{}{
		"funds":    new(big.Int).Div(balance, ether),
		"funded":   nonce,
		"requests": reqs,
	}, 3*time.Second); err != nil {
		log.Warn("Failed to send initial stats to client", "err", err)
		return
	}
	if err = send(wsconn, head, 3*time.Second); err != nil {
		log.Warn("Failed to send initial header to client", "err", err)
		return
	}
	// Keep reading requests from the websocket until the connection breaks
	for {
		// Fetch the next funding request and validate against github
		var msg struct {
			URL     string `json:"url"`
			Tier    uint   `json:"tier"`
			Captcha string `json:"captcha"`
			Symbol  string `json:"symbol"`
		}
		// not sure if it helps or not, but set a read deadline could help prevent resource leakage
		// if user did not give response for too long, then the routine will be stuck.
		conn.SetReadDeadline(time.Now().Add(wsReadTimeout))
		if err = conn.ReadJSON(&msg); err != nil {
			log.Debug("read json message failed", "err", err, "ip", ip)
			return
		}
		if !*noauthFlag && !strings.HasPrefix(msg.URL, "https://twitter.com/") && !strings.HasPrefix(msg.URL, "https://www.facebook.com/") {
			if err = sendError(wsconn, errors.New("URL doesn't link to supported services")); err != nil {
				log.Warn("Failed to send URL error to client", "err", err)
				return
			}
			continue
		}
		if msg.Tier >= uint(*tiersFlag) {
			//lint:ignore ST1005 This error is to be displayed in the browser
			if err = sendError(wsconn, errors.New("Invalid funding tier requested")); err != nil {
				log.Warn("Failed to send tier error to client", "err", err)
				return
			}
			continue
		}
		log.Info("Faucet funds requested", "url", msg.URL, "tier", msg.Tier, "ip", ip)

		// check #1: captcha verifications to exclude robot
		if *captchaToken != "" {
			form := url.Values{}
			form.Add("secret", *captchaSecret)
			form.Add("response", msg.Captcha)

			res, err := http.PostForm("https://hcaptcha.com/siteverify", form)
			if err != nil {
				if err = sendError(wsconn, err); err != nil {
					log.Warn("Failed to send captcha post error to client", "err", err)
					return
				}
				continue
			}
			var result struct {
				Success bool            `json:"success"`
				Errors  json.RawMessage `json:"error-codes"`
			}
			err = json.NewDecoder(res.Body).Decode(&result)
			res.Body.Close()
			if err != nil {
				if err = sendError(wsconn, err); err != nil {
					log.Warn("Failed to send captcha decode error to client", "err", err)
					return
				}
				continue
			}
			if !result.Success {
				log.Warn("Captcha verification failed", "err", string(result.Errors))
				//lint:ignore ST1005 it's funny and the robot won't mind
				if err = sendError(wsconn, errors.New("Beep-bop, you're a robot!")); err != nil {
					log.Warn("Failed to send captcha failure to client", "err", err)
					return
				}
				continue
			}
		}
		// Retrieve the Ethereum address to fund, the requesting user and a profile picture
		var (
			id       string
			username string
			avatar   string
			address  common.Address
		)
		switch {
		case strings.HasPrefix(msg.URL, "https://gist.github.com/"):
			if err = sendError(wsconn, errors.New("GitHub authentication discontinued at the official request of GitHub")); err != nil {
				log.Warn("Failed to send GitHub deprecation to client", "err", err)
				return
			}
			continue
		case strings.HasPrefix(msg.URL, "https://plus.google.com/"):
			//lint:ignore ST1005 Google is a company name and should be capitalized.
			if err = sendError(wsconn, errors.New("Google+ authentication discontinued as the service was sunset")); err != nil {
				log.Warn("Failed to send Google+ deprecation to client", "err", err)
				return
			}
			continue
		case strings.HasPrefix(msg.URL, "https://twitter.com/"):
			id, username, avatar, address, err = authTwitter(msg.URL, *twitterTokenV1Flag, *twitterTokenFlag)
		case strings.HasPrefix(msg.URL, "https://www.facebook.com/"):
			username, avatar, address, err = authFacebook(msg.URL)
			id = username
		case *noauthFlag:
			username, avatar, address, err = authNoAuth(msg.URL)
			id = username
		default:
			//lint:ignore ST1005 This error is to be displayed in the browser
			err = errors.New("Something funky happened, please open an issue at https://github.com/Ezkerrox/bsc/issues")
		}
		if err != nil {
			if err = sendError(wsconn, err); err != nil {
				log.Warn("Failed to send prefix error to client", "err", err)
				return
			}
			continue
		}

		// check #2: check IP and ID(address) to ensure the user didn't request funds too frequently
		f.lock.Lock()

		if ipTimeout := f.timeouts[ips[len(ips)-2]]; time.Now().Before(ipTimeout) {
			f.lock.Unlock()
			if err = sendError(wsconn, fmt.Errorf("%s left until next allowance", common.PrettyDuration(time.Until(ipTimeout)))); err != nil { // nolint: gosimple
				log.Warn("Failed to send funding error to client", "err", err)
				return
			}
			log.Info("too frequent funding(ip)", "TimeLeft", common.PrettyDuration(time.Until(ipTimeout)), "ip", ips[len(ips)-2], "ipsStr", ipsStr)
			continue
		}
		if idTimeout := f.timeouts[id]; time.Now().Before(idTimeout) {
			f.lock.Unlock()
			// Send an error if too frequent funding, otherwise a success
			if err = sendError(wsconn, fmt.Errorf("%s left until next allowance", common.PrettyDuration(time.Until(idTimeout)))); err != nil { // nolint: gosimple
				log.Warn("Failed to send funding error to client", "err", err)
				return
			}
			log.Info("too frequent funding(id)", "TimeLeft", common.PrettyDuration(time.Until(idTimeout)), "id", id)
			continue
		}
		// check #3: minimum mainnet balance check, internal error will bypass the check to avoid blocking the faucet service
		if f.clientMainnet != nil {
			mainnetAddr := address
			balanceMainnet, err := f.clientMainnet.BalanceAt(context.Background(), mainnetAddr, nil)
			if err != nil {
				log.Warn("check balance failed, call BalanceAt", "err", err)
			} else if balanceMainnet == nil {
				log.Warn("check balance failed, balanceMainnet is nil")
			} else {
				if balanceMainnet.Cmp(minMainnetBalance) < 0 {
					f.lock.Unlock()
					log.Warn("insufficient BNB on BSC mainnet", "address", mainnetAddr,
						"balanceMainnet", balanceMainnet, "minMainnetBalance", minMainnetBalance)
					// Send an error if failed to meet the minimum balance requirement
					if err = sendError(wsconn, fmt.Errorf("insufficient BNB on BSC mainnet        (require >=%sBNB)",
						weiToEtherStringFx(minMainnetBalance, 3))); err != nil {
						log.Warn("Failed to send mainnet minimum balance error to client", "err", err)
						return
					}
					continue
				}
			}
		}
		log.Info("Faucet request valid", "url", msg.URL, "tier", msg.Tier, "user", username, "address", address, "ip", ip)

		// now, it is ok to send tBNB or other tokens
		var tx *types.Transaction
		if msg.Symbol == "BNB" {
			// User wasn't funded recently, create the funding transaction
			amount := new(big.Int).Div(new(big.Int).Mul(big.NewInt(int64(*payoutFlag)), ether), big.NewInt(10))
			amount = new(big.Int).Mul(amount, new(big.Int).Exp(big.NewInt(5), big.NewInt(int64(msg.Tier)), nil))
			amount = new(big.Int).Div(amount, new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(msg.Tier)), nil))

			tx = types.NewTransaction(f.nonce+uint64(len(f.reqs)), address, amount, 21000, f.price, nil)
		} else {
			tokenInfo, ok := f.bep2eInfos[msg.Symbol]
			if !ok {
				f.lock.Unlock()
				log.Warn("Failed to find symbol", "symbol", msg.Symbol)
				continue
			}
			input, err := f.bep2eAbi.Pack("transfer", address, &tokenInfo.Amount)
			if err != nil {
				f.lock.Unlock()
				log.Warn("Failed to pack transfer transaction", "err", err)
				continue
			}
			tx = types.NewTransaction(f.nonce+uint64(len(f.reqs)), tokenInfo.Contract, nil, 420000, f.price, input)
		}
		signed, err := f.keystore.SignTx(f.account, tx, f.config.ChainID)
		if err != nil {
			f.lock.Unlock()
			if err = sendError(wsconn, err); err != nil {
				log.Warn("Failed to send transaction creation error to client", "err", err)
				return
			}
			continue
		}
		// Submit the transaction and mark as funded if successful
		if err := f.client.SendTransaction(context.Background(), signed); err != nil {
			f.lock.Unlock()
			if err = sendError(wsconn, err); err != nil {
				log.Warn("Failed to send transaction transmission error to client", "err", err)
				return
			}
			continue
		}
		f.reqs = append(f.reqs, &request{
			Avatar:  avatar,
			Account: address,
			Time:    time.Now(),
			Tx:      signed,
		})
		timeoutInt64 := time.Duration(*minutesFlag*int(math.Pow(3, float64(msg.Tier)))) * time.Minute
		grace := timeoutInt64 / 288 // 24h timeout => 5m grace

		f.timeouts[id] = time.Now().Add(timeoutInt64 - grace)
		f.timeouts[ips[len(ips)-2]] = time.Now().Add(timeoutInt64 - grace)
		f.lock.Unlock()
		if err = sendSuccess(wsconn, fmt.Sprintf("Funding request accepted for %s into %s", username, address.Hex())); err != nil {
			log.Warn("Failed to send funding success to client", "err", err)
			return
		}
		select {
		case f.update <- struct{}{}:
		default:
		}
	}
}

// refresh attempts to retrieve the latest header from the chain and extract the
// associated faucet balance and nonce for connectivity caching.
func (f *faucet) refresh(head *types.Header) error {
	// Ensure a state update does not run for too long
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// If no header was specified, use the current chain head
	var err error
	if head == nil {
		if head, err = f.client.HeaderByNumber(ctx, nil); err != nil {
			return err
		}
	}
	// Retrieve the balance, nonce and gas price from the current head
	var (
		balance *big.Int
		nonce   uint64
		price   *big.Int
	)
	if balance, err = f.client.BalanceAt(ctx, f.account.Address, head.Number); err != nil {
		return err
	}
	if nonce, err = f.client.NonceAt(ctx, f.account.Address, head.Number); err != nil {
		return err
	}
	if fixGasPrice != nil && *fixGasPrice > 0 {
		price = big.NewInt(*fixGasPrice)
	} else {
		if price, err = f.client.SuggestGasPrice(ctx); err != nil {
			return err
		}
	}
	// Everything succeeded, update the cached stats and eject old requests
	f.lock.Lock()
	f.head, f.balance = head, balance
	f.price, f.nonce = price, nonce
	if len(f.reqs) == 0 {
		log.Debug("refresh len(f.reqs) == 0", "f.nonce", f.nonce)
		f.lock.Unlock()
		return nil
	}
	if f.reqs[0].Tx.Nonce() == f.nonce {
		// if the next Tx failed to be included for a certain time(resendInterval), try to
		// resend it with higher gasPrice, as it could be discarded in the network.
		// Also resend extra following txs, as they could be discarded as well.
		if time.Now().After(f.reqs[0].Time.Add(resendInterval)) {
			for i, req := range f.reqs {
				if i >= resendBatchSize {
					break
				}
				prePrice := req.Tx.GasPrice()
				// bump gas price 20% to replace the previous tx
				newPrice := new(big.Int).Add(prePrice, new(big.Int).Div(prePrice, big.NewInt(5)))
				if newPrice.Cmp(resendMaxGasPrice) >= 0 {
					log.Info("resendMaxGasPrice reached", "newPrice", newPrice, "resendMaxGasPrice", resendMaxGasPrice, "nonce", req.Tx.Nonce())
					break
				}
				newTx := types.NewTransaction(req.Tx.Nonce(), *req.Tx.To(), req.Tx.Value(), req.Tx.Gas(), newPrice, req.Tx.Data())
				newSigned, err := f.keystore.SignTx(f.account, newTx, f.config.ChainID)
				if err != nil {
					log.Error("resend sign tx failed", "err", err)
				}
				log.Info("reqs[0] Tx has been stuck for a while, trigger resend",
					"resendInterval", resendInterval, "resendTxSize", resendBatchSize,
					"preHash", req.Tx.Hash().Hex(), "newHash", newSigned.Hash().Hex(),
					"newPrice", newPrice, "nonce", req.Tx.Nonce(), "req.Tx.Gas()", req.Tx.Gas())
				if err := f.client.SendTransaction(context.Background(), newSigned); err != nil {
					log.Warn("resend tx failed", "err", err)
					continue
				}
				req.Tx = newSigned
			}
		}
	}
	// it is abnormal that reqs[0] has larger nonce than next expected nonce.
	// could be caused by reorg? reset it
	if f.reqs[0].Tx.Nonce() > f.nonce {
		log.Warn("reset due to nonce gap", "f.nonce", f.nonce, "f.reqs[0].Tx.Nonce()", f.reqs[0].Tx.Nonce())
		f.reqs = f.reqs[:0]
	}
	// remove the reqs if they have smaller nonce, which means it is no longer valid,
	// either has been accepted or replaced.
	for len(f.reqs) > 0 && f.reqs[0].Tx.Nonce() < f.nonce {
		f.reqs = f.reqs[1:]
	}
	f.lock.Unlock()

	return nil
}

// loop keeps waiting for interesting events and pushes them out to connected
// websockets.
func (f *faucet) loop() {
	// Wait for chain events and push them to clients
	heads := make(chan *types.Header, 16)
	sub, err := f.client.SubscribeNewHead(context.Background(), heads)
	if err != nil {
		log.Crit("Failed to subscribe to head events", "err", err)
	}
	defer sub.Unsubscribe()

	// Start a goroutine to update the state from head notifications in the background
	update := make(chan *types.Header)

	go func() {
		for head := range update {
			// New chain head arrived, query the current stats and stream to clients
			timestamp := time.Unix(int64(head.Time), 0)
			if time.Since(timestamp) > time.Hour {
				log.Warn("Skipping faucet refresh, head too old", "number", head.Number, "hash", head.Hash(), "age", common.PrettyAge(timestamp))
				continue
			}
			if err := f.refresh(head); err != nil {
				log.Warn("Failed to update faucet state", "block", head.Number, "hash", head.Hash(), "err", err)
				continue
			}
			// Faucet state retrieved, update locally and send to clients
			f.lock.RLock()
			log.Info("Updated faucet state", "number", head.Number, "hash", head.Hash(), "age", common.PrettyAge(timestamp), "balance", f.balance, "nonce", f.nonce, "price", f.price)

			balance := new(big.Int).Div(f.balance, ether)

			for _, conn := range f.conns {
				go func(conn *wsConn) {
					if err := send(conn, map[string]interface{}{
						"funds":    balance,
						"funded":   f.nonce,
						"requests": f.reqs,
					}, time.Second); err != nil {
						log.Warn("Failed to send stats to client", "err", err)
						conn.conn.Close()
						return // Exit the goroutine if the first send fails
					}

					if err := send(conn, head, time.Second); err != nil {
						log.Warn("Failed to send header to client", "err", err)
						conn.conn.Close()
					}
				}(conn)
			}
			f.lock.RUnlock()
		}
	}()
	// Wait for various events and assign to the appropriate background threads
	for {
		select {
		case head := <-heads:
			// New head arrived, send if for state update if there's none running
			select {
			case update <- head:
			default:
			}

		case <-f.update:
			// Pending requests updated, stream to clients
			f.lock.RLock()
			for _, conn := range f.conns {
				go func(conn *wsConn) {
					if err := send(conn, map[string]interface{}{"requests": f.reqs}, time.Second); err != nil {
						log.Warn("Failed to send requests to client", "err", err)
						conn.conn.Close()
					}
				}(conn)
			}
			f.lock.RUnlock()
		}
	}
}

// sends transmits a data packet to the remote end of the websocket, but also
// setting a write deadline to prevent waiting forever on the node.
func send(conn *wsConn, value interface{}, timeout time.Duration) error {
	if timeout == 0 {
		timeout = 60 * time.Second
	}
	conn.wlock.Lock()
	defer conn.wlock.Unlock()
	conn.conn.SetWriteDeadline(time.Now().Add(timeout))
	return conn.conn.WriteJSON(value)
}

// sendError transmits an error to the remote end of the websocket, also setting
// the write deadline to 1 second to prevent waiting forever.
func sendError(conn *wsConn, err error) error {
	return send(conn, map[string]string{"error": err.Error()}, time.Second)
}

// sendSuccess transmits a success message to the remote end of the websocket, also
// setting the write deadline to 1 second to prevent waiting forever.
func sendSuccess(conn *wsConn, msg string) error {
	return send(conn, map[string]string{"success": msg}, time.Second)
}

// authTwitter tries to authenticate a faucet request using Twitter posts, returning
// the uniqueness identifier (user id/username), username, avatar URL and Ethereum address to fund on success.
func authTwitter(url string, tokenV1, tokenV2 string) (string, string, string, common.Address, error) {
	// Ensure the user specified a meaningful URL, no fancy nonsense
	parts := strings.Split(url, "/")
	if len(parts) < 4 || parts[len(parts)-2] != "status" {
		//lint:ignore ST1005 This error is to be displayed in the browser
		return "", "", "", common.Address{}, errors.New("Invalid Twitter status URL")
	}
	// Strip any query parameters from the tweet id and ensure it's numeric
	tweetID := strings.Split(parts[len(parts)-1], "?")[0]
	if !regexp.MustCompile("^[0-9]+$").MatchString(tweetID) {
		return "", "", "", common.Address{}, errors.New("Invalid Tweet URL")
	}
	// Twitter's API isn't really friendly with direct links.
	// It is restricted to 300 queries / 15 minute with an app api key.
	// Anything more will require read only authorization from the users and that we want to avoid.

	// If Twitter bearer token is provided, use the API, selecting the version
	// the user would prefer (currently there's a limit of 1 v2 app / developer
	// but unlimited v1.1 apps).
	switch {
	case tokenV1 != "":
		return authTwitterWithTokenV1(tweetID, tokenV1)
	case tokenV2 != "":
		return authTwitterWithTokenV2(tweetID, tokenV2)
	}
	// Twitter API token isn't provided so we just load the public posts
	// and scrape it for the Ethereum address and profile URL. We need to load
	// the mobile page though since the main page loads tweet contents via JS.
	url = strings.Replace(url, "https://twitter.com/", "https://mobile.twitter.com/", 1)

	res, err := http.Get(url)
	if err != nil {
		return "", "", "", common.Address{}, err
	}
	defer res.Body.Close()

	// Resolve the username from the final redirect, no intermediate junk
	parts = strings.Split(res.Request.URL.String(), "/")
	if len(parts) < 4 || parts[len(parts)-2] != "status" {
		//lint:ignore ST1005 This error is to be displayed in the browser
		return "", "", "", common.Address{}, errors.New("Invalid Twitter status URL")
	}
	username := parts[len(parts)-3]

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return "", "", "", common.Address{}, err
	}
	address := common.HexToAddress(string(regexp.MustCompile("0x[0-9a-fA-F]{40}").Find(body)))
	if address == (common.Address{}) {
		//lint:ignore ST1005 This error is to be displayed in the browser
		return "", "", "", common.Address{}, errors.New("No BNB Smart Chain address found to fund")
	}
	var avatar string
	if parts = regexp.MustCompile(`src="([^"]+twimg\.com/profile_images[^"]+)"`).FindStringSubmatch(string(body)); len(parts) == 2 {
		avatar = parts[1]
	}
	return username + "@twitter", username, avatar, address, nil
}

// authTwitterWithTokenV1 tries to authenticate a faucet request using Twitter's v1
// API, returning the user id, username, avatar URL and Ethereum address to fund on
// success.
func authTwitterWithTokenV1(tweetID string, token string) (string, string, string, common.Address, error) {
	// Query the tweet details from Twitter
	url := fmt.Sprintf("https://api.twitter.com/1.1/statuses/show.json?id=%s", tweetID)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return "", "", "", common.Address{}, err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", "", common.Address{}, err
	}
	defer res.Body.Close()

	var result struct {
		Text string `json:"text"`
		User struct {
			ID       string `json:"id_str"`
			Username string `json:"screen_name"`
			Avatar   string `json:"profile_image_url"`
		} `json:"user"`
	}
	err = json.NewDecoder(res.Body).Decode(&result)
	if err != nil {
		return "", "", "", common.Address{}, err
	}
	address := common.HexToAddress(regexp.MustCompile("0x[0-9a-fA-F]{40}").FindString(result.Text))
	if address == (common.Address{}) {
		//lint:ignore ST1005 This error is to be displayed in the browser
		return "", "", "", common.Address{}, errors.New("No Ethereum address found to fund")
	}
	return result.User.ID + "@twitter", result.User.Username, result.User.Avatar, address, nil
}

// authTwitterWithTokenV2 tries to authenticate a faucet request using Twitter's v2
// API, returning the user id, username, avatar URL and Ethereum address to fund on
// success.
func authTwitterWithTokenV2(tweetID string, token string) (string, string, string, common.Address, error) {
	// Query the tweet details from Twitter
	url := fmt.Sprintf("https://api.twitter.com/2/tweets/%s?expansions=author_id&user.fields=profile_image_url", tweetID)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return "", "", "", common.Address{}, err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", "", common.Address{}, err
	}
	defer res.Body.Close()

	var result struct {
		Data struct {
			AuthorID string `json:"author_id"`
			Text     string `json:"text"`
		} `json:"data"`
		Includes struct {
			Users []struct {
				ID       string `json:"id"`
				Username string `json:"username"`
				Avatar   string `json:"profile_image_url"`
			} `json:"users"`
		} `json:"includes"`
	}

	err = json.NewDecoder(res.Body).Decode(&result)
	if err != nil {
		return "", "", "", common.Address{}, err
	}

	address := common.HexToAddress(regexp.MustCompile("0x[0-9a-fA-F]{40}").FindString(result.Data.Text))
	if address == (common.Address{}) {
		//lint:ignore ST1005 This error is to be displayed in the browser
		return "", "", "", common.Address{}, errors.New("No Ethereum address found to fund")
	}
	return result.Data.AuthorID + "@twitter", result.Includes.Users[0].Username, result.Includes.Users[0].Avatar, address, nil
}

// authFacebook tries to authenticate a faucet request using Facebook posts,
// returning the username, avatar URL and Ethereum address to fund on success.
func authFacebook(url string) (string, string, common.Address, error) {
	// Ensure the user specified a meaningful URL, no fancy nonsense
	parts := strings.Split(strings.Split(url, "?")[0], "/")
	if parts[len(parts)-1] == "" {
		parts = parts[0 : len(parts)-1]
	}
	if len(parts) < 4 || parts[len(parts)-2] != "posts" {
		//lint:ignore ST1005 This error is to be displayed in the browser
		return "", "", common.Address{}, errors.New("Invalid Facebook post URL")
	}
	username := parts[len(parts)-3]

	// Facebook's Graph API isn't really friendly with direct links. Still, we don't
	// want to do ask read permissions from users, so just load the public posts and
	// scrape it for the Ethereum address and profile URL.
	//
	// Facebook recently changed their desktop webpage to use AJAX for loading post
	// content, so switch over to the mobile site for now. Will probably end up having
	// to use the API eventually.
	crawl := strings.Replace(url, "www.facebook.com", "m.facebook.com", 1)

	res, err := http.Get(crawl)
	if err != nil {
		return "", "", common.Address{}, err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return "", "", common.Address{}, err
	}
	address := common.HexToAddress(string(regexp.MustCompile("0x[0-9a-fA-F]{40}").Find(body)))
	if address == (common.Address{}) {
		//lint:ignore ST1005 This error is to be displayed in the browser
		return "", "", common.Address{}, errors.New("No BNB Smart Chain address found to fund. Please check the post URL and verify that it can be viewed publicly.")
	}
	var avatar string
	if parts = regexp.MustCompile(`src="([^"]+fbcdn\.net[^"]+)"`).FindStringSubmatch(string(body)); len(parts) == 2 {
		avatar = parts[1]
	}
	return username + "@facebook", avatar, address, nil
}

// authNoAuth tries to interpret a faucet request as a plain Ethereum address,
// without actually performing any remote authentication. This mode is prone to
// Byzantine attack, so only ever use for truly private networks.
func authNoAuth(url string) (string, string, common.Address, error) {
	address := common.HexToAddress(regexp.MustCompile("0x[0-9a-fA-F]{40}").FindString(url))
	if address == (common.Address{}) {
		//lint:ignore ST1005 This error is to be displayed in the browser
		return "", "", common.Address{}, errors.New("No BNB Smart Chain address found to fund")
	}
	return address.Hex() + "@noauth", "", address, nil
}

// getGenesis returns a genesis based on input args
func getGenesis(genesisFlag string, goerliFlag bool, sepoliaFlag bool) (*core.Genesis, error) {
	switch {
	case genesisFlag != "":
		var genesis core.Genesis
		err := common.LoadJSON(genesisFlag, &genesis)
		return &genesis, err
	default:
		return nil, errors.New("no genesis flag provided")
	}
}
