package distributed

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// KVUserFunctions is the NATS KV bucket for user-defined functions.
	KVUserFunctions = "wadjet_user_functions"
)

// UDFEntry is the JSON-serializable form of a UDF stored in NATS KV.
type UDFEntry struct {
	Name   string   `json:"name"`
	Params []string `json:"params"`
	Body   string   `json:"body"`
	Owner  string   `json:"owner,omitempty"`
	Locked bool     `json:"locked,omitempty"`
}

// NATSUDFStore manages UDF storage and propagation via NATS KV.
type NATSUDFStore struct {
	kv     jetstream.KeyValue
	store  *expr.UDFStore
	logger *slog.Logger
}

// NewNATSUDFStore creates a UDF store backed by NATS KV.
// It creates the KV bucket if it doesn't exist.
func NewNATSUDFStore(ctx context.Context, js jetstream.JetStream, store *expr.UDFStore, logger *slog.Logger) (*NATSUDFStore, error) {
	if logger == nil {
		logger = slog.Default()
	}

	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  KVUserFunctions,
		History: 3,
	})
	if err != nil {
		return nil, fmt.Errorf("creating UDF KV bucket: %w", err)
	}

	return &NATSUDFStore{
		kv:     kv,
		store:  store,
		logger: logger,
	}, nil
}

// Create stores a UDF in NATS KV, which will propagate to all watchers.
func (n *NATSUDFStore) Create(ctx context.Context, def expr.UDFDef, isAdmin bool) error {
	// First register locally to validate compilation
	if err := n.store.Register(def, isAdmin); err != nil {
		return err
	}

	entry := UDFEntry{
		Name:   def.Name,
		Params: def.Params,
		Body:   def.Body,
		Owner:  def.Owner,
		Locked: def.Locked,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshaling UDF: %w", err)
	}

	_, err = n.kv.Put(ctx, strings.ToLower(def.Name), data)
	if err != nil {
		return fmt.Errorf("storing UDF in NATS KV: %w", err)
	}

	n.logger.Info("UDF stored in NATS KV", "name", def.Name, "owner", def.Owner)
	return nil
}

// Delete removes a UDF from NATS KV.
func (n *NATSUDFStore) Delete(ctx context.Context, name, caller string, isAdmin bool) error {
	if err := n.store.Unregister(name, caller, isAdmin); err != nil {
		return err
	}

	err := n.kv.Delete(ctx, strings.ToLower(name))
	if err != nil {
		return fmt.Errorf("deleting UDF from NATS KV: %w", err)
	}

	n.logger.Info("UDF deleted from NATS KV", "name", name, "caller", caller)
	return nil
}

// List returns all UDFs from the local store.
func (n *NATSUDFStore) List() []expr.UDFDef {
	return n.store.List()
}

// LoadAll loads all UDFs from NATS KV into the local store.
// Called on startup to hydrate the store.
func (n *NATSUDFStore) LoadAll(ctx context.Context) error {
	keys, err := n.kv.Keys(ctx)
	if err != nil {
		// No keys yet is fine
		if err.Error() == "nats: no keys found" {
			return nil
		}
		return fmt.Errorf("listing UDF keys: %w", err)
	}

	// Load all entries, then register in dependency order
	var entries []UDFEntry
	for _, key := range keys {
		entry, err := n.kv.Get(ctx, key)
		if err != nil {
			n.logger.Warn("failed to load UDF", "key", key, "error", err)
			continue
		}
		var udf UDFEntry
		if err := json.Unmarshal(entry.Value(), &udf); err != nil {
			n.logger.Warn("failed to unmarshal UDF", "key", key, "error", err)
			continue
		}
		entries = append(entries, udf)
	}

	// Simple multi-pass registration to handle dependencies
	registered := make(map[string]bool)
	maxPasses := len(entries) + 1
	for pass := 0; pass < maxPasses && len(registered) < len(entries); pass++ {
		for _, e := range entries {
			if registered[e.Name] {
				continue
			}
			def := expr.UDFDef{
				Name:   e.Name,
				Params: e.Params,
				Body:   e.Body,
				Owner:  e.Owner,
				Locked: e.Locked,
			}
			// Register as admin to bypass permission checks during load
			if err := n.store.Register(def, true); err != nil {
				// Might fail due to dependency ordering — will retry next pass
				if pass == maxPasses-1 {
					n.logger.Warn("failed to register UDF after all passes",
						"name", e.Name, "error", err)
				}
				continue
			}
			registered[e.Name] = true
			n.logger.Info("loaded UDF from NATS KV", "name", e.Name)
		}
	}

	return nil
}

// Watch starts watching for UDF changes in NATS KV. When another node
// creates or deletes a UDF, this watcher applies the change locally.
// The returned cancel function stops the watcher.
func (n *NATSUDFStore) Watch(ctx context.Context) (context.CancelFunc, error) {
	watcher, err := n.kv.WatchAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("watching UDF KV: %w", err)
	}

	watchCtx, cancel := context.WithCancel(ctx)

	go func() {
		defer watcher.Stop()
		for {
			select {
			case <-watchCtx.Done():
				return
			case entry, ok := <-watcher.Updates():
				if !ok {
					return
				}
				if entry == nil {
					continue // initial values done marker
				}

				name := entry.Key()

				switch entry.Operation() {
				case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
					// Remove locally (as admin, since this is a propagated change)
					_ = n.store.Unregister(name, "", true)
					n.logger.Info("UDF removed via NATS watch", "name", name)

				case jetstream.KeyValuePut:
					var udf UDFEntry
					if err := json.Unmarshal(entry.Value(), &udf); err != nil {
						n.logger.Warn("failed to unmarshal watched UDF",
							"name", name, "error", err)
						continue
					}
					def := expr.UDFDef{
						Name:   udf.Name,
						Params: udf.Params,
						Body:   udf.Body,
						Owner:  udf.Owner,
						Locked: udf.Locked,
					}
					// Register as admin since this is a propagated change
					if err := n.store.Register(def, true); err != nil {
						n.logger.Warn("failed to register watched UDF",
							"name", name, "error", err)
					} else {
						n.logger.Debug("UDF updated via NATS watch", "name", name)
					}
				}
			}
		}
	}()

	return cancel, nil
}
