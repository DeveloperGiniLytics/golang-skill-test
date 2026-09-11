package jobs

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestHandler(s *Service) http.Handler {
	mux := http.NewServeMux()
	RegisterHandlers(mux, s)
	return mux
}

func doRequest(handler http.Handler, method, target, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func decodeJob(t *testing.T, rec *httptest.ResponseRecorder) Job {
	t.Helper()

	var job Job
	if err := json.Unmarshal(rec.Body.Bytes(), &job); err != nil {
		t.Fatalf("decode job: %v (body %q)", err, rec.Body.String())
	}
	return job
}

func decodeError(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()

	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode error body: %v (body %q)", err, rec.Body.String())
	}
	message, ok := payload["error"]
	if !ok || message == "" {
		t.Fatalf("expected an error message, got %q", rec.Body.String())
	}
	return message
}

func TestCreateJobEndpoint(t *testing.T) {
	s := NewService(1, 4, WithProcessor(noopProcessor()))
	defer s.Stop()
	handler := newTestHandler(s)

	rec := doRequest(handler, http.MethodPost, "/jobs", `{"payload":"hello"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("unexpected content type %q", got)
	}

	job := decodeJob(t, rec)
	if job.ID == "" {
		t.Fatal("expected a job id")
	}
	if job.Status != StatusQueued {
		t.Fatalf("expected queued, got %s", job.Status)
	}
	if job.Payload != "hello" {
		t.Fatalf("unexpected payload %q", job.Payload)
	}
}

func TestCreateJobRejectsBadRequests(t *testing.T) {
	s := NewService(1, 4, WithProcessor(noopProcessor()))
	defer s.Stop()
	handler := newTestHandler(s)

	cases := []struct {
		name string
		body string
	}{
		{"empty body", ""},
		{"malformed json", `{"payload":`},
		{"not an object", `"hello"`},
		{"wrong payload type", `{"payload":123}`},
		{"missing payload", `{}`},
		{"blank payload", `{"payload":""}`},
		{"whitespace payload", `{"payload":"   "}`},
		{"trailing garbage", `{"payload":"hello"} extra`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(handler, http.MethodPost, "/jobs", tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d (%s)", rec.Code, rec.Body.String())
			}
			decodeError(t, rec)
		})
	}
}

func TestCreateJobRejectsOversizedBody(t *testing.T) {
	s := NewService(1, 4, WithProcessor(noopProcessor()))
	defer s.Stop()
	handler := newTestHandler(s)

	body := `{"payload":"` + strings.Repeat("a", maxRequestBodyBytes+1) + `"}`
	rec := doRequest(handler, http.MethodPost, "/jobs", body)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d (%s)", rec.Code, rec.Body.String())
	}
	decodeError(t, rec)
}

func TestGetJobEndpoint(t *testing.T) {
	s := NewService(1, 4, WithProcessor(noopProcessor()))
	defer s.Stop()
	handler := newTestHandler(s)

	created := decodeJob(t, doRequest(handler, http.MethodPost, "/jobs", `{"payload":"hello"}`))

	deadline := time.Now().Add(3 * time.Second)
	for {
		rec := doRequest(handler, http.MethodGet, "/jobs/"+created.ID, "")
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d (%s)", rec.Code, rec.Body.String())
		}
		job := decodeJob(t, rec)
		if job.Status == StatusCompleted {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("job stuck in status %s", job.Status)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestGetJobEndpointReportsFailure(t *testing.T) {
	s := NewService(1, 4)
	defer s.Stop()
	handler := newTestHandler(s)

	created := decodeJob(t, doRequest(handler, http.MethodPost, "/jobs", `{"payload":"please fail"}`))

	deadline := time.Now().Add(3 * time.Second)
	for {
		job := decodeJob(t, doRequest(handler, http.MethodGet, "/jobs/"+created.ID, ""))
		if job.Status == StatusFailed {
			if job.Error == "" {
				t.Fatal("expected an error message on a failed job")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("job stuck in status %s", job.Status)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestGetUnknownJobEndpoint(t *testing.T) {
	s := NewService(1, 4, WithProcessor(noopProcessor()))
	defer s.Stop()
	handler := newTestHandler(s)

	rec := doRequest(handler, http.MethodGet, "/jobs/job-404", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d (%s)", rec.Code, rec.Body.String())
	}
	decodeError(t, rec)
}

func TestRoutingEdgeCases(t *testing.T) {
	s := NewService(1, 4, WithProcessor(noopProcessor()))
	defer s.Stop()
	handler := newTestHandler(s)

	created := decodeJob(t, doRequest(handler, http.MethodPost, "/jobs", `{"payload":"hello"}`))

	cases := []struct {
		name   string
		method string
		target string
		want   int
	}{
		{"list is not supported", http.MethodGet, "/jobs", http.StatusMethodNotAllowed},
		{"delete is not supported", http.MethodDelete, "/jobs/" + created.ID, http.StatusMethodNotAllowed},
		{"missing id", http.MethodGet, "/jobs/", http.StatusNotFound},
		{"nested path is not a job id", http.MethodGet, "/jobs/" + created.ID + "/status", http.StatusNotFound},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(handler, tc.method, tc.target, "")
			if rec.Code != tc.want {
				t.Fatalf("expected %d, got %d (%s)", tc.want, rec.Code, rec.Body.String())
			}
		})
	}
}

func TestCreateJobReturnsServiceUnavailableWhenQueueIsFull(t *testing.T) {
	processor, started, release := blockingProcessor()
	s := NewService(1, 1, WithProcessor(processor))
	defer s.Stop()
	defer close(release)
	handler := newTestHandler(s)

	if rec := doRequest(handler, http.MethodPost, "/jobs", `{"payload":"busy"}`); rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec.Code)
	}
	<-started
	if rec := doRequest(handler, http.MethodPost, "/jobs", `{"payload":"queued"}`); rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec.Code)
	}

	rec := doRequest(handler, http.MethodPost, "/jobs", `{"payload":"rejected"}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d (%s)", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("expected a Retry-After header")
	}
	decodeError(t, rec)
}

func TestCreateJobReturnsServiceUnavailableWhileStopping(t *testing.T) {
	s := NewService(1, 4, WithProcessor(noopProcessor()))
	handler := newTestHandler(s)
	s.Stop()

	rec := doRequest(handler, http.MethodPost, "/jobs", `{"payload":"hello"}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d (%s)", rec.Code, rec.Body.String())
	}
	decodeError(t, rec)
}

func TestCreateJobWithCanceledRequest(t *testing.T) {
	s := NewService(1, 4, WithProcessor(noopProcessor()))
	defer s.Stop()
	handler := newTestHandler(s)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"payload":"hello"}`)).WithContext(ctx)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestTimeout {
		t.Fatalf("expected 408, got %d (%s)", rec.Code, rec.Body.String())
	}
	decodeError(t, rec)
}

func TestConcurrentHTTPRequests(t *testing.T) {
	s := NewService(4, 32, WithProcessor(noopProcessor()))
	defer s.Stop()

	server := httptest.NewServer(newTestHandler(s))
	defer server.Close()

	const clients = 40
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			resp, err := server.Client().Post(server.URL+"/jobs", "application/json", strings.NewReader(`{"payload":"hello"}`))
			if err != nil {
				t.Errorf("post: %v", err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusServiceUnavailable {
				return
			}
			if resp.StatusCode != http.StatusCreated {
				t.Errorf("unexpected status %d", resp.StatusCode)
				return
			}

			var job Job
			if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
				t.Errorf("decode: %v", err)
				return
			}

			got, err := server.Client().Get(server.URL + "/jobs/" + job.ID)
			if err != nil {
				t.Errorf("get: %v", err)
				return
			}
			defer got.Body.Close()
			if got.StatusCode != http.StatusOK {
				t.Errorf("unexpected get status %d", got.StatusCode)
			}
		}()
	}
	wg.Wait()
}

func TestCreateJobPreservesPayloadVerbatim(t *testing.T) {
	s := NewService(1, 8, WithProcessor(noopProcessor()))
	defer s.Stop()
	handler := newTestHandler(s)

	cases := []struct {
		name string
		body string
		want string
	}{
		{"surrounding whitespace", `{"payload":"  hello  "}`, "  hello  "},
		{"unicode", `{"payload":"héllo 🙂"}`, "héllo 🙂"},
		{"quotes and newlines", `{"payload":"line1\nline2 \"quoted\""}`, "line1\nline2 \"quoted\""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(handler, http.MethodPost, "/jobs", tc.body)
			if rec.Code != http.StatusCreated {
				t.Fatalf("expected 201, got %d (%s)", rec.Code, rec.Body.String())
			}
			if got := decodeJob(t, rec).Payload; got != tc.want {
				t.Fatalf("payload %q was rewritten to %q", tc.want, got)
			}
		})
	}
}

func TestCreateJobReturnsUniqueIDs(t *testing.T) {
	s := NewService(2, 64, WithProcessor(noopProcessor()))
	defer s.Stop()
	handler := newTestHandler(s)

	seen := make(map[string]struct{})
	for i := 0; i < 32; i++ {
		job := decodeJob(t, doRequest(handler, http.MethodPost, "/jobs", `{"payload":"hello"}`))
		if job.Status != StatusQueued {
			t.Fatalf("expected a new job to start queued, got %s", job.Status)
		}
		if _, dup := seen[job.ID]; dup {
			t.Fatalf("duplicate id %s", job.ID)
		}
		seen[job.ID] = struct{}{}
	}
}
