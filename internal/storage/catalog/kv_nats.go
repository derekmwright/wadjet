package catalog

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

const kvBucket = "caelum_catalog"

// NATSKVAdapter wraps a NATS JetStream KeyValue as a MetaKV.
type NATSKVAdapter struct {
	kv jetstream.KeyValue
}

// NewNATSKV creates a NATS KV-backed MetaKV.
// Creates or opens the caelum_catalog KV bucket.
func NewNATSKV(js jetstream.JetStream) (*NATSKVAdapter, error) {
	kv, err := js.CreateOrUpdateKeyValue(context.Background(), jetstream.KeyValueConfig{
		Bucket:      kvBucket,
		Description: "Caelum catalog metadata",
		Storage:     jetstream.FileStorage,
	})
	if err != nil {
		return nil, fmt.Errorf("creating catalog KV bucket: %w", err)
	}
	return &NATSKVAdapter{kv: kv}, nil
}

func (n *NATSKVAdapter) Get(key string) ([]byte, uint64, error) {
	entry, err := n.kv.Get(context.Background(), key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, 0, ErrKeyNotFound
		}
		return nil, 0, fmt.Errorf("kv get %q: %w", key, err)
	}
	return entry.Value(), entry.Revision(), nil
}

func (n *NATSKVAdapter) Put(key string, value []byte) (uint64, error) {
	rev, err := n.kv.Put(context.Background(), key, value)
	if err != nil {
		return 0, fmt.Errorf("kv put %q: %w", key, err)
	}
	return rev, nil
}

func (n *NATSKVAdapter) Delete(key string) error {
	err := n.kv.Delete(context.Background(), key)
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return fmt.Errorf("kv delete %q: %w", key, err)
	}
	return nil
}

func (n *NATSKVAdapter) List(prefix string) ([]string, error) {
	allKeys, err := n.kv.Keys(context.Background())
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("kv list keys: %w", err)
	}
	if prefix == "" {
		return allKeys, nil
	}
	var filtered []string
	for _, k := range allKeys {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			filtered = append(filtered, k)
		}
	}
	return filtered, nil
}
