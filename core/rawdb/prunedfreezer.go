package rawdb

import (
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Ezkerrox/bsc/common"
	"github.com/Ezkerrox/bsc/ethdb"
	"github.com/Ezkerrox/bsc/log"
	"github.com/Ezkerrox/bsc/params"
	"github.com/prometheus/tsdb/fileutil"
)

// prunedfreezer not contain ancient data, only record 'frozen' , the next recycle block number form kvstore.
type prunedfreezer struct {
	db ethdb.KeyValueStore // Meta database
	// WARNING: The `frozen` field is accessed atomically. On 32 bit platforms, only
	// 64-bit aligned fields can be atomic. The struct is guaranteed to be so aligned,
	// so take advantage of that (https://golang.org/pkg/sync/atomic/#pkg-note-BUG).
	frozen    uint64 // BlockNumber of next frozen block
	threshold uint64 // Number of recent blocks not to freeze (params.FullImmutabilityThreshold apart from tests)

	instanceLock fileutil.Releaser // File-system lock to prevent double opens
	quit         chan struct{}
	closeOnce    sync.Once
}

// newPrunedFreezer creates a chain freezer that deletes data enough ‘old’.
func newPrunedFreezer(datadir string, db ethdb.KeyValueStore, offset uint64) (*prunedfreezer, error) {
	if info, err := os.Lstat(datadir); !os.IsNotExist(err) {
		if info.Mode()&os.ModeSymlink != 0 {
			log.Warn("Symbolic link ancient database is not supported", "path", datadir)
			return nil, errSymlinkDatadir
		}
	}

	lock, _, err := fileutil.Flock(filepath.Join(datadir, "../NODATA_ANCIENT_FLOCK"))
	if err != nil {
		return nil, err
	}

	freezer := &prunedfreezer{
		db:           db,
		frozen:       offset,
		threshold:    params.FullImmutabilityThreshold,
		instanceLock: lock,
		quit:         make(chan struct{}),
	}

	if err := freezer.repair(datadir); err != nil {
		return nil, err
	}

	// delete ancient dir
	if err := os.RemoveAll(datadir); err != nil && !os.IsNotExist(err) {
		log.Warn("Failed to remove the ancient dir", "path", datadir, "error", err)
		return nil, err
	}
	log.Info("Opened ancientdb with nodata mode", "database", datadir, "frozen", freezer.frozen)
	return freezer, nil
}

// repair init frozen , compatible disk-ancientdb and pruner-block-tool.
func (f *prunedfreezer) repair(datadir string) error {
	offset := atomic.LoadUint64(&f.frozen)
	// compatible freezer
	minItems := uint64(math.MaxUint64)
	for name, disableSnappy := range chainFreezerNoSnappy {
		var (
			table *freezerTable
			err   error
		)
		if slices.Contains(additionTables, name) {
			table, err = newAdditionTable(datadir, name, disableSnappy, false)
		} else {
			table, err = newFreezerTable(datadir, name, disableSnappy, false)
		}
		if err != nil {
			return err
		}
		// addition tables only align head
		if slices.Contains(additionTables, name) {
			if EmptyTable(table) {
				continue
			}
		}
		items := table.items.Load()
		if minItems > items {
			minItems = items
		}
		table.Close()
	}

	// If the dataset has undergone a prune block, the offset is a non-zero value, otherwise the offset is a zero value.
	// The minItems is the value relative to offset
	offset += minItems

	// FrozenOfAncientFreezer is the progress of the last prune-freezer freeze.
	frozenInDB := ReadFrozenOfAncientFreezer(f.db)
	maxOffset := max(offset, frozenInDB)
	log.Info("Read ancient db item counts", "items", minItems, "frozen", maxOffset)

	atomic.StoreUint64(&f.frozen, maxOffset)
	if err := f.SyncAncient(); err != nil {
		return nil
	}
	return nil
}

// Close terminates the chain prunedfreezer.
func (f *prunedfreezer) Close() error {
	var err error
	f.closeOnce.Do(func() {
		close(f.quit)
		f.SyncAncient()
		err = f.instanceLock.Release()
	})
	return err
}

// HasAncient returns an indicator whether the specified ancient data exists, return nil.
func (f *prunedfreezer) HasAncient(kind string, number uint64) (bool, error) {
	return false, nil
}

// Ancient retrieves an ancient binary blob from prunedfreezer, return nil.
func (f *prunedfreezer) Ancient(kind string, number uint64) ([]byte, error) {
	if _, ok := chainFreezerNoSnappy[kind]; ok {
		if number >= atomic.LoadUint64(&f.frozen) {
			return nil, errOutOfBounds
		}
		return nil, nil
	}
	return nil, errUnknownTable
}

// Ancients returns the last of the frozen items.
func (f *prunedfreezer) Ancients() (uint64, error) {
	return atomic.LoadUint64(&f.frozen), nil
}

// ItemAmountInAncient returns the actual length of current ancientDB, return 0.
func (f *prunedfreezer) ItemAmountInAncient() (uint64, error) {
	return 0, nil
}

// AncientOffSet returns the offset of current ancientDB, offset == frozen.
func (f *prunedfreezer) AncientOffSet() uint64 {
	return atomic.LoadUint64(&f.frozen)
}

// AncientDatadir returns an error as we don't have a backing chain freezer.
func (f *prunedfreezer) AncientDatadir() (string, error) {
	return "", errNotSupported
}

// Tail returns the number of first stored item in the freezer.
func (f *prunedfreezer) Tail() (uint64, error) {
	return atomic.LoadUint64(&f.frozen), nil
}

// AncientSize returns the ancient size of the specified category, return 0.
func (f *prunedfreezer) AncientSize(kind string) (uint64, error) {
	if _, ok := chainFreezerNoSnappy[kind]; ok {
		return 0, nil
	}
	return 0, errUnknownTable
}

// AppendAncient update frozen.
//
// Notably, this function is lock free but kind of thread-safe. All out-of-order
// injection will be rejected. But if two injections with same number happen at
// the same time, we can get into the trouble.
func (f *prunedfreezer) AppendAncient(number uint64, hash, header, body, receipts, td []byte) (err error) {
	if atomic.LoadUint64(&f.frozen) != number {
		return errOutOrderInsertion
	}
	atomic.AddUint64(&f.frozen, 1)
	return nil
}

// TruncateAncients discards any recent data above the provided threshold number, always success.
func (f *prunedfreezer) TruncateHead(items uint64) (uint64, error) {
	preHead := atomic.LoadUint64(&f.frozen)
	if preHead > items {
		atomic.StoreUint64(&f.frozen, items)
		WriteFrozenOfAncientFreezer(f.db, atomic.LoadUint64(&f.frozen))
	}
	return preHead, nil
}

// TruncateTail discards any recent data below the provided threshold number.
func (f *prunedfreezer) TruncateTail(tail uint64) (uint64, error) {
	return 0, errNotSupported
}

// SyncAncient flushes meta data tables to disk.
func (f *prunedfreezer) SyncAncient() error {
	WriteFrozenOfAncientFreezer(f.db, atomic.LoadUint64(&f.frozen))
	// compatible offline prune blocks tool
	WriteOffSetOfCurrentAncientFreezer(f.db, atomic.LoadUint64(&f.frozen))
	return nil
}

// freeze is a background thread that periodically checks the blockchain for any
// import progress and moves ancient data from the fast database into the freezer.
//
// This functionality is deliberately broken off from block importing to avoid
// incurring additional data shuffling delays on block propagation.
func (f *prunedfreezer) freeze() {
	nfdb := &nofreezedb{KeyValueStore: f.db}

	var backoff bool
	for {
		select {
		case <-f.quit:
			log.Info("Freezer shutting down")
			return
		default:
		}
		if backoff {
			select {
			case <-time.NewTimer(freezerRecheckInterval).C:
			case <-f.quit:
				return
			}
		}

		// Retrieve the freezing threshold.
		hash := ReadHeadBlockHash(nfdb)
		if hash == (common.Hash{}) {
			log.Debug("Current full block hash unavailable") // new chain, empty database
			backoff = true
			continue
		}
		number := ReadHeaderNumber(nfdb, hash)
		threshold := atomic.LoadUint64(&f.threshold)

		switch {
		case number == nil:
			log.Error("Current full block number unavailable", "hash", hash)
			backoff = true
			continue

		case *number < threshold:
			log.Debug("Current full block not old enough", "number", *number, "hash", hash, "delay", threshold)
			backoff = true
			continue

		case *number-threshold <= f.frozen:
			log.Debug("Ancient blocks frozen already", "number", *number, "hash", hash, "frozen", f.frozen)
			backoff = true
			continue
		}
		head := ReadHeader(nfdb, hash, *number)
		if head == nil {
			log.Error("Stable state block unavailable", "number", *number, "hash", hash)
			backoff = true
			continue
		}

		stableStabeNumber := ReadSafePointBlockNumber(nfdb)
		switch {
		case stableStabeNumber < params.StableStateThreshold:
			log.Debug("Stable state block not old enough", "number", stableStabeNumber)
			backoff = true
			continue

		case stableStabeNumber > *number:
			log.Warn("Stable state block biger current full block", "number", stableStabeNumber, "number", *number)
			backoff = true
			continue
		}
		stableStabeNumber -= params.StableStateThreshold

		// Seems we have data ready to be frozen, process in usable batches
		limit := *number - threshold
		if limit > stableStabeNumber {
			limit = stableStabeNumber
		}

		if limit < f.frozen {
			log.Debug("Stable state block has prune", "limit", limit, "frozen", f.frozen)
			backoff = true
			continue
		}

		if limit-f.frozen > freezerBatchLimit {
			limit = f.frozen + freezerBatchLimit
		}
		var (
			start    = time.Now()
			first    = f.frozen
			ancients = make([]common.Hash, 0, limit-f.frozen)
		)
		for f.frozen <= limit {
			// Retrieves all the components of the canonical block
			hash := ReadCanonicalHash(nfdb, f.frozen)
			if hash == (common.Hash{}) {
				log.Error("Canonical hash missing, can't freeze", "number", f.frozen)
			}
			log.Trace("Deep froze ancient block", "number", f.frozen, "hash", hash)
			// Inject all the components into the relevant data tables
			if err := f.AppendAncient(f.frozen, nil, nil, nil, nil, nil); err != nil {
				log.Error("Append ancient err", "number", f.frozen, "hash", hash, "err", err)
				break
			}
			// may include common.Hash{}, will be delete in gcKvStore
			ancients = append(ancients, hash)
		}
		// Batch of blocks have been frozen, flush them before wiping from leveldb
		if err := f.SyncAncient(); err != nil {
			log.Crit("Failed to flush frozen tables", "err", err)
		}
		backoff = f.frozen-first >= freezerBatchLimit
		gcKvStore(f.db, ancients, first, f.frozen, start)
	}
}

func (f *prunedfreezer) SetupFreezerEnv(env *ethdb.FreezerEnv) error {
	return nil
}

func (f *prunedfreezer) ReadAncients(fn func(ethdb.AncientReaderOp) error) (err error) {
	return fn(f)
}

func (f *prunedfreezer) AncientRange(kind string, start, count, maxBytes uint64) ([][]byte, error) {
	return nil, errNotSupported
}

func (f *prunedfreezer) ModifyAncients(func(ethdb.AncientWriteOp) error) (int64, error) {
	return 0, errNotSupported
}

// TruncateTableTail will truncate certain table to new tail
func (f *prunedfreezer) TruncateTableTail(kind string, tail uint64) (uint64, error) {
	return 0, errNotSupported
}

// ResetTable will reset certain table with new start point
func (f *prunedfreezer) ResetTable(kind string, startAt uint64, onlyEmpty bool) error {
	return errNotSupported
}
