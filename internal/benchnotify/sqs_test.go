package benchnotify

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func TestRegionFromQueueURL(t *testing.T) {
	tests := []struct {
		url  string
		want string
	}{
		{"https://sqs.us-east-2.amazonaws.com/123456789012/wadjet-bench-events", "us-east-2"},
		{"https://sqs.eu-west-1.amazonaws.com/123456789012/q", "eu-west-1"},
		{"https://us-east-2.queue.amazonaws.com/123456789012/q", "us-east-2"},
		{"http://localhost:4566/000000000000/q", ""},
		{"", ""},
		{"not a url at all", ""},
	}
	for _, tt := range tests {
		if got := RegionFromQueueURL(tt.url); got != tt.want {
			t.Errorf("RegionFromQueueURL(%q) = %q, want %q", tt.url, got, tt.want)
		}
	}
}

type fakeSQS struct {
	inputs []*sqs.SendMessageInput
	err    error
}

func (f *fakeSQS) SendMessage(_ context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.inputs = append(f.inputs, in)
	if f.err != nil {
		return nil, f.err
	}
	return &sqs.SendMessageOutput{}, nil
}

func TestSQSEmitterSendsJSONBody(t *testing.T) {
	fake := &fakeSQS{}
	em := &sqsEmitter{client: fake, queueURL: "https://sqs.us-east-2.amazonaws.com/1/wadjet-bench-events"}
	n := NewWithEmitter(em, Config{RunID: "20260817-142233"})

	n.Send(Event{Event: EventQueryCompleted, Query: "Q05", WallSeconds: Seconds(0), Rows: Rows(0), OK: OK(false)})

	if len(fake.inputs) != 1 {
		t.Fatalf("SendMessage calls = %d, want 1", len(fake.inputs))
	}
	if got := *fake.inputs[0].QueueUrl; got != "https://sqs.us-east-2.amazonaws.com/1/wadjet-bench-events" {
		t.Errorf("QueueUrl = %q", got)
	}
	var ev Event
	if err := json.Unmarshal([]byte(*fake.inputs[0].MessageBody), &ev); err != nil {
		t.Fatalf("message body is not the event JSON: %v", err)
	}
	if ev.RunID != "20260817-142233" || ev.Query != "Q05" || ev.OK == nil || *ev.OK {
		t.Errorf("round-tripped event = %+v", ev)
	}
}

func TestSQSEmitterWrapsSendError(t *testing.T) {
	em := &sqsEmitter{client: &fakeSQS{err: errors.New("AccessDenied")}, queueURL: "q"}
	err := em.Emit(context.Background(), Event{Event: EventFatal})
	if err == nil {
		t.Fatal("Emit returned nil, want the wrapped SDK error")
	}
	if got := err.Error(); got != "sqs send: AccessDenied" {
		t.Errorf("error = %q, want it wrapped with context", got)
	}
}
