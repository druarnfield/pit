package loghub

import (
	"testing"
	"time"
)

func TestHub_PublishAndSubscribe(t *testing.T) {
	h := New()
	defer h.Close()

	h.Activate("run-1")
	ch := h.Subscribe("run-1")

	now := time.Now()
	entry := Entry{
		Timestamp: now,
		RunID:     "run-1",
		DAGName:   "my_dag",
		TaskName:  "task_a",
		Level:     "INFO",
		Message:   "hello world",
		Attempt:   1,
		Duration:  "2s",
	}
	h.Publish("run-1", entry)

	select {
	case got := <-ch:
		if got.RunID != "run-1" {
			t.Errorf("RunID = %q, want %q", got.RunID, "run-1")
		}
		if got.DAGName != "my_dag" {
			t.Errorf("DAGName = %q, want %q", got.DAGName, "my_dag")
		}
		if got.TaskName != "task_a" {
			t.Errorf("TaskName = %q, want %q", got.TaskName, "task_a")
		}
		if got.Level != "INFO" {
			t.Errorf("Level = %q, want %q", got.Level, "INFO")
		}
		if got.Message != "hello world" {
			t.Errorf("Message = %q, want %q", got.Message, "hello world")
		}
		if got.Attempt != 1 {
			t.Errorf("Attempt = %d, want %d", got.Attempt, 1)
		}
		if got.Duration != "2s" {
			t.Errorf("Duration = %q, want %q", got.Duration, "2s")
		}
		if !got.Timestamp.Equal(now) {
			t.Errorf("Timestamp = %v, want %v", got.Timestamp, now)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for entry")
	}
}

func TestHub_MultipleSubscribers(t *testing.T) {
	h := New()
	defer h.Close()

	h.Activate("run-1")
	ch1 := h.Subscribe("run-1")
	ch2 := h.Subscribe("run-1")

	entry := Entry{
		RunID:   "run-1",
		Message: "broadcast",
	}
	h.Publish("run-1", entry)

	for i, ch := range []<-chan Entry{ch1, ch2} {
		select {
		case got := <-ch:
			if got.Message != "broadcast" {
				t.Errorf("subscriber %d: Message = %q, want %q", i, got.Message, "broadcast")
			}
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d: timed out waiting for entry", i)
		}
	}
}

func TestHub_Complete(t *testing.T) {
	h := New()
	defer h.Close()

	h.Activate("run-1")
	ch := h.Subscribe("run-1")

	h.Complete("run-1", "success")

	// Channel should be closed — reads should return zero value immediately.
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected channel to be closed after Complete")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for channel close")
	}

	status, done := h.RunStatus("run-1")
	if !done {
		t.Error("RunStatus done = false, want true")
	}
	if status != "success" {
		t.Errorf("RunStatus status = %q, want %q", status, "success")
	}
}

func TestHub_IsActive(t *testing.T) {
	h := New()
	defer h.Close()

	if h.IsActive("run-1") {
		t.Error("IsActive before Activate = true, want false")
	}

	h.Activate("run-1")
	if !h.IsActive("run-1") {
		t.Error("IsActive after Activate = false, want true")
	}

	h.Complete("run-1", "success")
	if h.IsActive("run-1") {
		t.Error("IsActive after Complete = true, want false")
	}
}

func TestHub_UnsubscribeCleanup(t *testing.T) {
	h := New()
	defer h.Close()

	h.Activate("run-1")
	ch1 := h.Subscribe("run-1")
	ch2 := h.Subscribe("run-1")

	h.Unsubscribe("run-1", ch1)

	entry := Entry{
		RunID:   "run-1",
		Message: "after unsub",
	}
	h.Publish("run-1", entry)

	// ch1 should NOT receive the entry (was unsubscribed).
	select {
	case <-ch1:
		t.Error("unsubscribed channel ch1 received an entry")
	default:
	}

	// ch2 should still receive the entry.
	select {
	case got := <-ch2:
		if got.Message != "after unsub" {
			t.Errorf("ch2 Message = %q, want %q", got.Message, "after unsub")
		}
	case <-time.After(time.Second):
		t.Fatal("ch2: timed out waiting for entry")
	}
}

func TestHub_SubscribeAfterComplete(t *testing.T) {
	h := New()
	defer h.Close()

	h.Activate("run-1")
	h.Complete("run-1", "failed")

	ch := h.Subscribe("run-1")

	// Channel should already be closed.
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected channel to be closed for completed run")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for closed channel")
	}
}
