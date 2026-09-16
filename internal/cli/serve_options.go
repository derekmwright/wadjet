package cli

import "time"

// ServeOptions is the resolved value of every flag the distributed serve
// modes read.
//
// It exists because those modes live in internal/clid, which may not reach
// into this package's flag variables: the command tree, the flags and the
// configuration precedence that resolves them are the embedded binary's
// (MIT), and the coordinator, the worker and the run functions that build
// them are not (AGPL — see LICENSING.md). One struct crossing that line
// keeps the flag registrations in one place and the seam visible.
//
// Read it with ServeOptionsNow, and only AFTER ApplyServeRuntimeEnvelope:
// several of these fields are auto-detected there and are still zero before
// it runs.
type ServeOptions struct {
	AggPartialSplit       bool
	AsyncScratchPurge     bool
	BackgroundCompaction  bool
	BoundedDirtyWrites    bool
	BroadcastBytes        int64
	Bucket                string
	CacheBytes            int64
	ClusterID             string
	ConfigFile            string
	CoordDataPlane        string
	DataPlane             string
	DataPlaneAddr         string
	DecodedCacheBytes     int64
	DrainTimeout          time.Duration
	EagerDispatch         bool
	EnableAlerts          bool
	GRPCAddr              string
	HTTPAddr              string
	LateMaterialization   bool
	LocalFastPathBytes    int64
	LocalityPlacement     bool
	MaxConcurrent         int
	MaxConcurrentQry      int
	MemoryBudget          int64
	MetricsAddr           string
	MmapRelief            bool
	MmapReliefThresholdMB int64
	MorselWorkers         int
	NATSPort              int
	NATSURL               string
	PeerExchangeAddr      string
	PeerExchangeAdvertise string
	PeerWireCompression   bool
	PGAddr                string
	PGTLSCert             string
	PGTLSKey              string
	QueryIntermediateGC   time.Duration
	QueryIntermediateTTL  time.Duration
	QueryTimeout          string
	ReclaimDroppedTables  bool
	ResultStoreBytes      int64
	ScanDecodeAhead       bool
	ScanDecodeAheadBytes  int64
	SharedPoolBudget      int64
	ShuffleDecodeAhead    bool
	ShuffleDurability     string
	SkewSplit             bool
	SortMergeJoinBytes    int64
	SpillDir              string
	SpillFloatingBudget   bool
	StreamingExchange     bool
	StreamingShuffleRead  bool
}

// ServeOptionsNow snapshots the resolved flag values. Call it inside RunE,
// after the envelope has been applied — never at registration time, where
// every field still holds its registered default.
func ServeOptionsNow() ServeOptions {
	return ServeOptions{
		AggPartialSplit:       aggPartialSplit,
		AsyncScratchPurge:     asyncScratchPurge,
		BackgroundCompaction:  backgroundCompaction,
		BoundedDirtyWrites:    boundedDirtyWrites,
		BroadcastBytes:        broadcastBytes,
		Bucket:                bucket,
		CacheBytes:            cacheBytes,
		ClusterID:             clusterID,
		ConfigFile:            configFile,
		CoordDataPlane:        coordDataPlane,
		DataPlane:             dataPlane,
		DataPlaneAddr:         dataPlaneAddr,
		DecodedCacheBytes:     decodedCacheBytes,
		DrainTimeout:          drainTimeout,
		EagerDispatch:         eagerDispatch,
		EnableAlerts:          enableAlerts,
		GRPCAddr:              grpcAddr,
		HTTPAddr:              httpAddr,
		LateMaterialization:   lateMaterialization,
		LocalFastPathBytes:    localFastPathBytes,
		LocalityPlacement:     localityPlacement,
		MaxConcurrent:         maxConcurrent,
		MaxConcurrentQry:      maxConcurrentQry,
		MemoryBudget:          memoryBudget,
		MetricsAddr:           metricsAddr,
		MmapRelief:            mmapRelief,
		MmapReliefThresholdMB: mmapReliefThresholdMB,
		MorselWorkers:         morselWorkers,
		NATSPort:              natsPort,
		NATSURL:               natsURL,
		PeerExchangeAddr:      peerExchangeAddr,
		PeerExchangeAdvertise: peerExchangeAdvertise,
		PeerWireCompression:   peerWireCompression,
		PGAddr:                pgAddr,
		PGTLSCert:             pgTLSCert,
		PGTLSKey:              pgTLSKey,
		QueryIntermediateGC:   queryIntermediateGC,
		QueryIntermediateTTL:  queryIntermediateTTL,
		QueryTimeout:          queryTimeout,
		ReclaimDroppedTables:  reclaimDroppedTables,
		ResultStoreBytes:      resultStoreBytes,
		ScanDecodeAhead:       scanDecodeAhead,
		ScanDecodeAheadBytes:  scanDecodeAheadBytes,
		SharedPoolBudget:      sharedPoolBudget,
		ShuffleDecodeAhead:    shuffleDecodeAhead,
		ShuffleDurability:     shuffleDurability,
		SkewSplit:             skewSplit,
		SortMergeJoinBytes:    sortMergeJoinBytes,
		SpillDir:              spillDir,
		SpillFloatingBudget:   spillFloatingBudget,
		StreamingExchange:     streamingExchange,
		StreamingShuffleRead:  streamingShuffleRead,
	}
}

// ServeMode is the resolved run mode: standalone, coordinator or worker.
func ServeMode() string { return serveMode() }
