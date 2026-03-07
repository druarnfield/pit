package loghub

import (
	"testing"
	"time"
)

func TestWriter_SingleLine(t *testing.T) {
	h := New()
	h.Activate("run-1")
	ch := h.Subscribe("run-1")

	w := NewWriter(h, "run-1", "dag-a", "task-x", 1)
	_, _ = w.Write([]byte("hello world\n"))

	select {
	case e := <-ch:
		if e.Message != "hello world" {
			t.Errorf("Message = %q, want %q", e.Message, "hello world")
		}
		if e.RunID != "run-1" {
			t.Errorf("RunID = %q, want %q", e.RunID, "run-1")
		}
		if e.DAGName != "dag-a" {
			t.Errorf("DAGName = %q, want %q", e.DAGName, "dag-a")
		}
		if e.TaskName != "task-x" {
			t.Errorf("TaskName = %q, want %q", e.TaskName, "task-x")
		}
		if e.Attempt != 1 {
			t.Errorf("Attempt = %d, want %d", e.Attempt, 1)
		}
		if e.Level != "info" {
			t.Errorf("Level = %q, want %q", e.Level, "info")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for entry")
	}
}

func TestWriter_MultipleLines(t *testing.T) {
	h := New()
	h.Activate("run-1")
	ch := h.Subscribe("run-1")

	w := NewWriter(h, "run-1", "dag-a", "task-x", 1)
	_, _ = w.Write([]byte("line one\nline two\n"))

	var messages []string
	for i := 0; i < 2; i++ {
		select {
		case e := <-ch:
			messages = append(messages, e.Message)
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for entry %d", i)
		}
	}

	if messages[0] != "line one" {
		t.Errorf("messages[0] = %q, want %q", messages[0], "line one")
	}
	if messages[1] != "line two" {
		t.Errorf("messages[1] = %q, want %q", messages[1], "line two")
	}
}

func TestWriter_PartialLine(t *testing.T) {
	h := New()
	h.Activate("run-1")
	ch := h.Subscribe("run-1")

	w := NewWriter(h, "run-1", "dag-a", "task-x", 1)
	_, _ = w.Write([]byte("hel"))
	_, _ = w.Write([]byte("lo\n"))

	select {
	case e := <-ch:
		if e.Message != "hello" {
			t.Errorf("Message = %q, want %q", e.Message, "hello")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for entry")
	}
}

func TestWriter_Duration(t *testing.T) {
	h := New()
	h.Activate("run-1")
	ch := h.Subscribe("run-1")

	w := NewWriter(h, "run-1", "dag-a", "task-x", 1)
	time.Sleep(10 * time.Millisecond)
	_, _ = w.Write([]byte("tick\n"))

	select {
	case e := <-ch:
		if e.Duration == "" || e.Duration == "0s" {
			t.Errorf("Duration = %q, want non-zero", e.Duration)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for entry")
	}
}

func TestWriter_ErrorLevel(t *testing.T) {
	h := New()
	h.Activate("run-1")
	ch := h.Subscribe("run-1")

	w := NewWriter(h, "run-1", "dag-a", "task-x", 1)
	_, _ = w.Write([]byte("ERROR: something broke\n"))

	select {
	case e := <-ch:
		if e.Level != "error" {
			t.Errorf("Level = %q, want %q", e.Level, "error")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for entry")
	}
}

func TestWriter_SetAttempt(t *testing.T) {
	h := New()
	h.Activate("run-1")
	ch := h.Subscribe("run-1")

	w := NewWriter(h, "run-1", "dag-a", "task-x", 1)
	w.SetAttempt(3)
	_, _ = w.Write([]byte("retry\n"))

	select {
	case e := <-ch:
		if e.Attempt != 3 {
			t.Errorf("Attempt = %d, want %d", e.Attempt, 3)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for entry")
	}
}

func TestWriter_Flush(t *testing.T) {
	h := New()
	h.Activate("run-1")
	ch := h.Subscribe("run-1")

	w := NewWriter(h, "run-1", "dag-a", "task-x", 1)
	_, _ = w.Write([]byte("no newline"))
	w.Flush()

	select {
	case e := <-ch:
		if e.Message != "no newline" {
			t.Errorf("Message = %q, want %q", e.Message, "no newline")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for entry")
	}
}

func TestDetectLevel(t *testing.T) {
	tests := []struct {
		msg  string
		want string
	}{
		{"normal message", "info"},
		{"ERROR: broke", "error"},
		{"error: broke", "error"},
		{"Traceback (most recent call last):", "error"},
		{"WARNING: check", "warn"},
		{"warning: check", "warn"},
	}

	for _, tt := range tests {
		t.Run(tt.msg, func(t *testing.T) {
			got := DetectLevel(tt.msg)
			if got != tt.want {
				t.Errorf("DetectLevel(%q) = %q, want %q", tt.msg, got, tt.want)
			}
		})
	}
}
