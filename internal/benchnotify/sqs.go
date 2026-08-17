package benchnotify

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// sendMessageAPI is the one SQS call this package makes.
type sendMessageAPI interface {
	SendMessage(ctx context.Context, in *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

// sqsEmitter publishes each event as one SQS message body.
type sqsEmitter struct {
	client   sendMessageAPI
	queueURL string
}

func (e *sqsEmitter) Emit(ctx context.Context, ev Event) error {
	body, err := ev.Marshal()
	if err != nil {
		return fmt.Errorf("marshaling event: %w", err)
	}
	_, err = e.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(e.queueURL),
		MessageBody: aws.String(string(body)),
	})
	if err != nil {
		return fmt.Errorf("sqs send: %w", err)
	}
	return nil
}

// New builds an SQS-backed Notifier. An empty cfg.QueueURL — the default of
// --notify-sqs-url — returns nil, the disabled notifier, so callers need no
// enabled/disabled branch. A client that cannot be built logs a warning and
// also returns nil: notifications are never a gate on the benchmark.
func New(ctx context.Context, cfg Config) *Notifier {
	if cfg.QueueURL == "" {
		return nil
	}
	logf := cfg.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}

	region := cfg.Region
	if region == "" {
		region = RegionFromQueueURL(cfg.QueueURL)
	}
	var opts []func(*config.LoadOptions) error
	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		logf("WARNING: benchnotify: loading AWS config (notifications disabled): %v", err)
		return nil
	}
	if awsCfg.Region == "" {
		logf("WARNING: benchnotify: no region for %s (notifications disabled); pass --notify-sqs-region", cfg.QueueURL)
		return nil
	}

	n := NewWithEmitter(&sqsEmitter{client: sqs.NewFromConfig(awsCfg), queueURL: cfg.QueueURL}, cfg)
	logf("benchnotify: pushing run events to %s (run_id=%s, region=%s)", cfg.QueueURL, n.RunID(), awsCfg.Region)
	return n
}

// RegionFromQueueURL extracts the region from an SQS queue URL. Both the
// current host form (sqs.<region>.amazonaws.com) and the legacy one
// (<region>.queue.amazonaws.com) are recognized; anything else returns ""
// and the caller falls back to the ambient AWS config.
func RegionFromQueueURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	host := u.Hostname()
	parts := strings.Split(host, ".")
	if len(parts) < 3 {
		return ""
	}
	switch {
	case parts[0] == "sqs":
		return parts[1]
	case parts[1] == "queue":
		return parts[0]
	}
	return ""
}
