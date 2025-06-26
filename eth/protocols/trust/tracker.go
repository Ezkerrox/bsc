package trust

import (
	"time"

	"github.com/Ezkerrox/bsc/p2p/tracker"
)

// requestTracker is a singleton tracker for request times.
var requestTracker = tracker.New(ProtocolName, time.Minute)
