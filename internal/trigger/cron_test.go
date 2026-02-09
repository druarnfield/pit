package trigger

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestNewCronTrigger_InvalidSchedule(t *testing.T) {
	_, err := NewCronTrigger("test", "not a schedule")
	if err == nil {
		t.Error("NewCronTrigger() expected error for invalid schedule, got nil")
	}
	if !strings.Contains(err.Error(), "invalid cron schedule") {
		t.Errorf("error = %q, want it to contain 'invalid cron schedule'", err)
	}
}

func TestNewCronTrigger_ValidSchedules(t *testing.T) {
	schedules := []string{
		"0 6 * * *",
		"*/5 * * * *",
		"@every 10s",
		"@hourly",
	}
	for _, s := range schedules {
		t.Run(s, func(t *testing.T) {
			ct, err := NewCronTrigger("test", s)
			if err != nil {
				t.Fatalf("NewCronTrigger(%q) error: %v", s, err)
			}
			if ct == nil {
				t.Fatal("NewCronTrigger() returned nil")
			}
		})
	}
}

func TestCronTrigger_Name(t *testing.T) {
	ct, err := NewCronTrigger("my_dag", "0 6 * * *")
	if err != nil {
		t.Fatal(err)
	}
	name := ct.Name()
	if !strings.Contains(name, "cron") || !strings.Contains(name, "my_dag") {
		t.Errorf("Name() = %q, want it to contain 'cron' and 'my_dag'", name)
	}
}

func TestCronTrigger_Start_Delivers(t *testing.T) {
	ct, err := NewCronTrigger("test_dag", "@every 100ms")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	events := make(chan Event, 10)
	go ct.Start(ctx, events)

	select {
	case ev := <-events:
		if ev.DAGName != "test_dag" {
			t.Errorf("event.DAGName = %q, want %q", ev.DAGName, "test_dag")
		}
		if ev.Source != "cron" {
			t.Errorf("event.Source = %q, want %q", ev.Source, "cron")
		}
		if len(ev.Files) != 0 {
			t.Errorf("event.Files = %v, want empty", ev.Files)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for cron event")
	}
}

func TestCronTrigger_Start_CancelStops(t *testing.T) {
	ct, err := NewCronTrigger("test_dag", "@every 100ms")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan Event, 10)

	done := make(chan struct{})
	go func() {
		ct.Start(ctx, events)
		close(done)
	}()

	// Wait for at least one event
	select {
	case <-events:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial event")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start() did not return after cancel")
	}
}
