package physical

// exchangeVariantFor returns a skeleton Exchange Stage whose Type and
// payload satisfy the given required distribution. It does not set ID,
// Dependencies, ClusterID, Tasks, or Distribution — callers fill those
// in. Returns ok=false when the requirement is RequiredAny (no exchange
// needed).
func exchangeVariantFor(req RequiredDistribution) (Stage, bool) {
	switch req.Kind {
	case RequiredAny:
		return Stage{}, false
	case RequiredBroadcast:
		return Stage{Type: StageExchangeReplicate}, true
	case RequiredSingleton:
		return Stage{Type: StageExchangeGather}, true
	case RequiredHashPartitionedOn, RequiredClusteredOn:
		return Stage{
			Type:          StageExchangeRepartition,
			ShuffleKeys:   append([]string(nil), req.Keys...),
			NumPartitions: req.Count,
		}, true
	default:
		return Stage{}, false
	}
}
