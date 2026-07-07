package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// AlertMeta is the catalog entry for a CREATE ALERT definition.
// Stored at key "<clusterID>.alert.<name>" via MetaKV CAS.
type AlertMeta struct {
	Name            string            `json:"name"`
	QueryText       string            `json:"query"`
	IntervalSeconds int64             `json:"interval_seconds"`
	WebhookURL      string            `json:"webhook_url,omitempty"`
	WebhookHeaders  map[string]string `json:"webhook_headers,omitempty"`
	InsertIntoTable string            `json:"insert_into_table,omitempty"`
	Enabled         bool              `json:"enabled"`
	CreatedAt       time.Time         `json:"created_at"`
	CreatedBy       string            `json:"created_by,omitempty"`
	// Creator identity snapshot for definer's-rights scheduled execution: the
	// alert query runs under this identity's ABAC subject on every tick (see
	// auth.IdentitySnapshot). Empty on alerts created before this existed —
	// the scheduler treats those fail-closed under enabled auth.
	CreatedByRole   string            `json:"created_by_role,omitempty"`
	CreatedByMethod string            `json:"created_by_method,omitempty"`
	CreatedByAttrs  map[string]string `json:"created_by_attrs,omitempty"`
	LastEvaluatedAt time.Time         `json:"last_evaluated_at,omitempty"`
	Version         int64             `json:"version"`
}

const alertKeyPrefix = "alert."

// CreateAlert writes a new alert entry; fails if an alert with the same name exists.
func (c *Catalog) CreateAlert(_ context.Context, m AlertMeta) error {
	if m.Name == "" {
		return fmt.Errorf("alert name is required")
	}
	key := c.key(alertKeyPrefix + m.Name)
	if _, _, err := c.kv.Get(key); err == nil {
		return fmt.Errorf("alert %q already exists", m.Name)
	} else if err != ErrKeyNotFound {
		return err
	}
	m.Version = 1
	if m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now().UTC()
	}
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	_, err = c.kv.Put(key, data)
	return err
}

// GetAlert returns the AlertMeta for name; returns an error if missing.
func (c *Catalog) GetAlert(_ context.Context, name string) (*AlertMeta, error) {
	key := c.key(alertKeyPrefix + name)
	data, _, err := c.kv.Get(key)
	if err != nil {
		if err == ErrKeyNotFound {
			return nil, fmt.Errorf("alert %q not found", name)
		}
		return nil, err
	}
	var m AlertMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// DropAlert removes the alert entry; no error if missing.
func (c *Catalog) DropAlert(_ context.Context, name string) error {
	return c.kv.Delete(c.key(alertKeyPrefix + name))
}

// SetAlertEnabled toggles the enabled flag via CAS. Retries on revision mismatch.
func (c *Catalog) SetAlertEnabled(ctx context.Context, name string, enabled bool) error {
	return c.mutateAlert(ctx, name, func(m *AlertMeta) {
		m.Enabled = enabled
	})
}

// TouchAlertEvaluated updates LastEvaluatedAt; retries on CAS conflict.
// Failure to update is non-fatal for the scheduler; callers log and move on.
func (c *Catalog) TouchAlertEvaluated(ctx context.Context, name string, at time.Time) error {
	return c.mutateAlert(ctx, name, func(m *AlertMeta) {
		m.LastEvaluatedAt = at.UTC()
	})
}

// ListAlerts returns all alert entries, sorted by name.
func (c *Catalog) ListAlerts(_ context.Context) ([]AlertMeta, error) {
	prefix := c.key(alertKeyPrefix)
	keys, err := c.kv.List(prefix)
	if err != nil {
		return nil, err
	}
	var alerts []AlertMeta
	for _, k := range keys {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		data, _, err := c.kv.Get(k)
		if err != nil {
			continue
		}
		var m AlertMeta
		if err := json.Unmarshal(data, &m); err != nil {
			continue
		}
		alerts = append(alerts, m)
	}
	sort.Slice(alerts, func(i, j int) bool { return alerts[i].Name < alerts[j].Name })
	return alerts, nil
}

// mutateAlert reads, mutates, and writes an alert with CAS retry.
func (c *Catalog) mutateAlert(_ context.Context, name string, fn func(*AlertMeta)) error {
	key := c.key(alertKeyPrefix + name)
	const maxRetries = 10
	for attempt := 0; attempt < maxRetries; attempt++ {
		data, rev, err := c.kv.Get(key)
		if err != nil {
			if err == ErrKeyNotFound {
				return fmt.Errorf("alert %q not found", name)
			}
			return err
		}
		var m AlertMeta
		if err := json.Unmarshal(data, &m); err != nil {
			return err
		}
		fn(&m)
		m.Version++
		out, err := json.Marshal(m)
		if err != nil {
			return err
		}
		if _, err := c.kv.Update(key, out, rev); err == nil {
			return nil
		} else if err != ErrRevisionMismatch {
			return err
		}
		casBackoff(attempt)
	}
	return fmt.Errorf("alert %q: exceeded CAS retries", name)
}
