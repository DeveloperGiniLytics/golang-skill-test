package jobs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Status string

const (
	StatusQueued     Status = "queued"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
)

var (
	ErrQueueFull = errors.New("job queue is full")
	ErrStopping  = errors.New("service is shutting down")
)

const defaultShutdownGrace = 5 * time.Second

type Job struct {
	ID        string    `json:"id"`
	Payload   string    `json:"payload"`
	Status    Status    `json:"status"`
	Error     string    `json:"error,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type Processor interface {
	Process(ctx context.Context, payload string) error
}

type ProcessorFunc func(context.Context, string) error

func (f ProcessorFunc) Process(ctx context.Context, payload string) error {
	return f(ctx, payload)
}

type Option func(*Service)

func WithProcessor(p Processor) Option {
	return func(s *Service) {
		if p != nil {
			s.processor = p
		}
	}
}

func WithShutdownGrace(d time.Duration) Option {
	return func(s *Service) {
		if d > 0 {
			s.shutdownGrace = d
		}
	}
}

type Service struct {
	mu   sync.RWMutex
	jobs map[string]*Job

	stopMu  sync.RWMutex
	stopped bool
	queue   chan string

	workers       int
	processor     Processor
	shutdownGrace time.Duration

	wg       sync.WaitGroup
	sequence atomic.Uint64
	stopOnce sync.Once

	ctx    context.Context
	cancel context.CancelFunc
}

func NewService(workers, queueCapacity int, opts ...Option) *Service {
	if workers < 1 {
		workers = 1
	}
	if queueCapacity < 1 {
		queueCapacity = 1
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		jobs:          make(map[string]*Job),
		queue:         make(chan string, queueCapacity),
		workers:       workers,
		processor:     ProcessorFunc(defaultProcessor),
		shutdownGrace: defaultShutdownGrace,
		ctx:           ctx,
		cancel:        cancel,
	}
	for _, opt := range opts {
		opt(s)
	}

	s.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go s.worker()
	}
	return s
}

func defaultProcessor(ctx context.Context, payload string) error {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-ctx.Done():
		return ctx.Err()
	}
	if payload == "" {
		return errors.New("empty payload")
	}
	if strings.Contains(payload, "fail") {
		return errors.New("simulated processing failure")
	}
	return nil
}

func (s *Service) Create(ctx context.Context, payload string) (*Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	job := &Job{
		ID:        fmt.Sprintf("job-%d", s.sequence.Add(1)),
		Payload:   payload,
		Status:    StatusQueued,
		CreatedAt: time.Now().UTC(),
	}

	s.stopMu.RLock()
	defer s.stopMu.RUnlock()
	if s.stopped {
		return nil, ErrStopping
	}

	s.mu.Lock()
	s.jobs[job.ID] = job
	s.mu.Unlock()

	select {
	case s.queue <- job.ID:
		return cloneJob(job), nil
	default:
		s.mu.Lock()
		delete(s.jobs, job.ID)
		s.mu.Unlock()
		return nil, ErrQueueFull
	}
}

func (s *Service) Get(id string) (*Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	job, ok := s.jobs[id]
	if !ok {
		return nil, false
	}
	return cloneJob(job), true
}

func (s *Service) Stop() {
	s.stopOnce.Do(func() {
		s.stopMu.Lock()
		s.stopped = true
		close(s.queue)
		s.stopMu.Unlock()

		drained := make(chan struct{})
		go func() {
			s.wg.Wait()
			close(drained)
		}()

		timer := time.NewTimer(s.shutdownGrace)
		defer timer.Stop()

		select {
		case <-drained:
		case <-timer.C:
			s.cancel()
			<-drained
		}
		s.cancel()
	})
}

func (s *Service) worker() {
	defer s.wg.Done()

	for id := range s.queue {
		s.process(id)
	}
}

func (s *Service) process(id string) {
	s.mu.Lock()
	job, ok := s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return
	}
	job.Status = StatusProcessing
	job.Error = ""
	payload := job.Payload
	s.mu.Unlock()

	err := s.runProcessor(payload)

	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		job.Status = StatusFailed
		job.Error = err.Error()
		return
	}
	job.Status = StatusCompleted
}

func (s *Service) runProcessor(payload string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("job processing panicked: %v", r)
		}
	}()
	return s.processor.Process(s.ctx, payload)
}

func cloneJob(j *Job) *Job {
	clone := *j
	return &clone
}
