package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func blockingProcessor() (Processor, chan struct{}, chan struct{}) {
	started := make(chan struct{}, 1024)
	release := make(chan struct{})
	p := ProcessorFunc(func(ctx context.Context, payload string) error {
		started <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	return p, started, release
}

func noopProcessor() Processor {
	return ProcessorFunc(func(ctx context.Context, payload string) error {
		return nil
	})
}

func waitForStatus(t *testing.T, s *Service, id string, want Status) *Job {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, ok := s.Get(id)
		if ok && got.Status == want {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
	got, _ := s.Get(id)
	t.Fatalf("job %s did not reach status %s (last: %+v)", id, want, got)
	return nil
}

func TestCreateAndGet(t *testing.T) {
	s := NewService(1, 4)
	defer s.Stop()

	job, err := s.Create(context.Background(), "hello")
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != StatusQueued {
		t.Fatalf("expected queued, got %s", job.Status)
	}
	if job.ID == "" || job.Payload != "hello" || job.CreatedAt.IsZero() {
		t.Fatalf("unexpected job: %+v", job)
	}

	got, ok := s.Get(job.ID)
	if !ok || got.ID != job.ID {
		t.Fatalf("job not found")
	}
	if got == job {
		t.Fatal("Get must return a copy, not the stored job")
	}
}

func TestGetUnknownJob(t *testing.T) {
	s := NewService(1, 4)
	defer s.Stop()

	if _, ok := s.Get("job-does-not-exist"); ok {
		t.Fatal("expected lookup to fail")
	}
}

func TestJobIDsAreUnique(t *testing.T) {
	s := NewService(2, 200, WithProcessor(noopProcessor()))
	defer s.Stop()

	var mu sync.Mutex
	seen := make(map[string]struct{})

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			job, err := s.Create(context.Background(), "hello")
			if err != nil {
				t.Errorf("create: %v", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if _, dup := seen[job.ID]; dup {
				t.Errorf("duplicate id %s", job.ID)
			}
			seen[job.ID] = struct{}{}
		}()
	}
	wg.Wait()
}

func TestJobLifecycle(t *testing.T) {
	processor, started, release := blockingProcessor()
	s := NewService(1, 4, WithProcessor(processor))
	defer s.Stop()

	job, err := s.Create(context.Background(), "hello")
	if err != nil {
		t.Fatal(err)
	}

	<-started
	waitForStatus(t, s, job.ID, StatusProcessing)

	close(release)
	completed := waitForStatus(t, s, job.ID, StatusCompleted)
	if completed.Error != "" {
		t.Fatalf("unexpected error on completed job: %s", completed.Error)
	}
}

func TestFailedJob(t *testing.T) {
	s := NewService(1, 4)
	defer s.Stop()

	job, err := s.Create(context.Background(), "please fail")
	if err != nil {
		t.Fatal(err)
	}

	failed := waitForStatus(t, s, job.ID, StatusFailed)
	if failed.Error == "" {
		t.Fatal("expected failure message")
	}
}

func TestProcessorPanicFailsJobOnly(t *testing.T) {
	s := NewService(1, 4, WithProcessor(ProcessorFunc(func(ctx context.Context, payload string) error {
		if payload == "boom" {
			panic("processor exploded")
		}
		return nil
	})))
	defer s.Stop()

	panicking, err := s.Create(context.Background(), "boom")
	if err != nil {
		t.Fatal(err)
	}
	failed := waitForStatus(t, s, panicking.ID, StatusFailed)
	if failed.Error == "" {
		t.Fatal("expected failure message")
	}

	next, err := s.Create(context.Background(), "hello")
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, s, next.ID, StatusCompleted)
}

func TestQueueFullReturnsError(t *testing.T) {
	processor, started, release := blockingProcessor()
	s := NewService(1, 1, WithProcessor(processor))
	defer s.Stop()
	defer close(release)

	if _, err := s.Create(context.Background(), "occupies the worker"); err != nil {
		t.Fatal(err)
	}
	<-started

	if _, err := s.Create(context.Background(), "fills the queue"); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := s.Create(context.Background(), "rejected")
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, ErrQueueFull) {
			t.Fatalf("expected ErrQueueFull, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Create blocked instead of rejecting a full queue")
	}
}

func TestRejectedJobIsNotStored(t *testing.T) {
	processor, started, release := blockingProcessor()
	s := NewService(1, 1, WithProcessor(processor))
	defer s.Stop()
	defer close(release)

	if _, err := s.Create(context.Background(), "occupies the worker"); err != nil {
		t.Fatal(err)
	}
	<-started
	if _, err := s.Create(context.Background(), "fills the queue"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(context.Background(), "rejected"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}

	s.mu.RLock()
	stored := len(s.jobs)
	s.mu.RUnlock()
	if stored != 2 {
		t.Fatalf("expected 2 stored jobs, got %d", stored)
	}
}

func TestCreateWithCanceledContext(t *testing.T) {
	s := NewService(1, 4)
	defer s.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := s.Create(ctx, "hello"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	s.mu.RLock()
	stored := len(s.jobs)
	s.mu.RUnlock()
	if stored != 0 {
		t.Fatalf("expected no stored jobs, got %d", stored)
	}
}

func TestCreateAfterStop(t *testing.T) {
	s := NewService(2, 4)
	s.Stop()

	if _, err := s.Create(context.Background(), "hello"); !errors.Is(err, ErrStopping) {
		t.Fatalf("expected ErrStopping, got %v", err)
	}
}

func TestStopDrainsQueuedJobs(t *testing.T) {
	s := NewService(2, 32, WithProcessor(noopProcessor()))

	ids := make([]string, 0, 16)
	for i := 0; i < 16; i++ {
		job, err := s.Create(context.Background(), "hello")
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, job.ID)
	}

	s.Stop()

	for _, id := range ids {
		job, ok := s.Get(id)
		if !ok {
			t.Fatalf("job %s missing after shutdown", id)
		}
		if job.Status != StatusCompleted {
			t.Fatalf("job %s left in status %s after shutdown", id, job.Status)
		}
	}
}

func TestStopCancelsStuckProcessorAfterGrace(t *testing.T) {
	processor, started, release := blockingProcessor()
	defer close(release)

	s := NewService(1, 4, WithProcessor(processor), WithShutdownGrace(50*time.Millisecond))

	job, err := s.Create(context.Background(), "stuck")
	if err != nil {
		t.Fatal(err)
	}
	<-started

	done := make(chan struct{})
	go func() {
		s.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not return after the shutdown grace period")
	}

	got, ok := s.Get(job.ID)
	if !ok || got.Status != StatusFailed {
		t.Fatalf("expected stuck job to fail, got %+v", got)
	}
}

func TestStopIsIdempotentUnderConcurrency(t *testing.T) {
	s := NewService(4, 16)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Stop()
		}()
	}
	wg.Wait()
}

func TestConcurrentCreateStopAndGet(t *testing.T) {
	s := NewService(4, 64, WithProcessor(noopProcessor()))

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			job, err := s.Create(context.Background(), "hello")
			if err != nil {
				if !errors.Is(err, ErrQueueFull) && !errors.Is(err, ErrStopping) {
					t.Errorf("unexpected create error: %v", err)
				}
				return
			}
			if _, ok := s.Get(job.ID); !ok {
				t.Errorf("created job %s not readable", job.ID)
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		s.Stop()
	}()

	wg.Wait()
}

func TestWorkersExitAfterStop(t *testing.T) {
	before := runtime.NumGoroutine()

	s := NewService(8, 16, WithProcessor(noopProcessor()))
	for i := 0; i < 16; i++ {
		if _, err := s.Create(context.Background(), "hello"); err != nil && !errors.Is(err, ErrQueueFull) {
			t.Fatal(err)
		}
	}
	s.Stop()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("goroutines leaked: before=%d after=%d", before, runtime.NumGoroutine())
}

func TestWorkerPoolProcessesConcurrentlyUpToWorkerCount(t *testing.T) {
	const workers = 4

	var active, peak atomic.Int64
	started := make(chan struct{}, 64)
	release := make(chan struct{})

	processor := ProcessorFunc(func(ctx context.Context, payload string) error {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			observed := peak.Load()
			if current <= observed || peak.CompareAndSwap(observed, current) {
				break
			}
		}
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	})

	s := NewService(workers, 32, WithProcessor(processor))
	defer s.Stop()
	defer close(release)

	for i := 0; i < workers*3; i++ {
		if _, err := s.Create(context.Background(), "hello"); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < workers; i++ {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatalf("only %d of %d workers picked up a job", i, workers)
		}
	}

	select {
	case <-started:
		t.Fatal("more jobs in flight than configured workers")
	case <-time.After(100 * time.Millisecond):
	}

	if got := peak.Load(); got != workers {
		t.Fatalf("expected %d concurrent workers, peaked at %d", workers, got)
	}
}

func TestNoGoroutinePerJob(t *testing.T) {
	const workers = 2

	processor, started, release := blockingProcessor()
	s := NewService(workers, 256, WithProcessor(processor))
	defer s.Stop()
	defer close(release)

	for i := 0; i < workers; i++ {
		if _, err := s.Create(context.Background(), "warm up"); err != nil {
			t.Fatal(err)
		}
		<-started
	}
	baseline := runtime.NumGoroutine()

	for i := 0; i < 200; i++ {
		if _, err := s.Create(context.Background(), "hello"); err != nil {
			t.Fatal(err)
		}
	}

	if got := runtime.NumGoroutine(); got > baseline+workers {
		t.Fatalf("queueing 200 jobs spawned goroutines: baseline=%d now=%d", baseline, got)
	}
}

func TestDefaultProcessorBehavior(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		wantErr bool
	}{
		{"plain payload completes", "hello", false},
		{"payload named fail", "fail", true},
		{"fail as a substring", "please fail now", true},
		{"fail at the end", "do not fail", true},
		{"uppercase is not a failure", "FAIL", false},
		{"empty payload", "", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := defaultProcessor(context.Background(), tc.payload)
			if tc.wantErr && err == nil {
				t.Fatal("expected an error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestDefaultProcessorRespectsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	if err := defaultProcessor(ctx, "hello"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("processor ignored cancellation for %s", elapsed)
	}
}

func TestNewServiceClampsInvalidConfiguration(t *testing.T) {
	s := NewService(0, 0, WithProcessor(nil), WithShutdownGrace(0))
	defer s.Stop()

	if s.workers != 1 {
		t.Fatalf("expected at least 1 worker, got %d", s.workers)
	}
	if got := cap(s.queue); got != 1 {
		t.Fatalf("expected a queue capacity of at least 1, got %d", got)
	}
	if s.processor == nil {
		t.Fatal("expected the default processor to be kept")
	}
	if s.shutdownGrace != defaultShutdownGrace {
		t.Fatalf("expected the default shutdown grace, got %s", s.shutdownGrace)
	}
}

func TestGetReturnsIndependentCopy(t *testing.T) {
	processor, started, release := blockingProcessor()
	s := NewService(1, 4, WithProcessor(processor))
	defer s.Stop()
	defer close(release)

	job, err := s.Create(context.Background(), "hello")
	if err != nil {
		t.Fatal(err)
	}
	<-started

	snapshot, ok := s.Get(job.ID)
	if !ok {
		t.Fatal("job not found")
	}
	snapshot.Status = StatusCompleted
	snapshot.Payload = "tampered"

	fresh, ok := s.Get(job.ID)
	if !ok {
		t.Fatal("job not found")
	}
	if fresh.Status != StatusProcessing || fresh.Payload != "hello" {
		t.Fatalf("stored job was mutated through a returned copy: %+v", fresh)
	}
}

func TestJobJSONContract(t *testing.T) {
	data, err := json.Marshal(&Job{
		ID:        "job-1",
		Payload:   "hello",
		Status:    StatusQueued,
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}

	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"id", "payload", "status", "created_at"} {
		if _, ok := fields[key]; !ok {
			t.Fatalf("missing %q in %s", key, data)
		}
	}
	if _, ok := fields["error"]; ok {
		t.Fatalf("error must be omitted when empty: %s", data)
	}

	failed, err := json.Marshal(&Job{ID: "job-2", Status: StatusFailed, Error: "boom"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(failed), `"error":"boom"`) {
		t.Fatalf("expected an error field on a failed job: %s", failed)
	}
}
