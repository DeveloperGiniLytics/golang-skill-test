package jobs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

const maxRequestBodyBytes = 64 << 10

type createJobRequest struct {
	Payload string `json:"payload"`
}

func RegisterHandlers(mux *http.ServeMux, service *Service) {
	mux.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		handleCreateJob(w, r, service)
	})
	mux.HandleFunc("GET /jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		handleGetJob(w, r, service)
	})
}

func handleCreateJob(w http.ResponseWriter, r *http.Request, service *Service) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)

	var req createJobRequest
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if strings.TrimSpace(req.Payload) == "" {
		writeError(w, http.StatusBadRequest, "payload is required")
		return
	}

	job, err := service.Create(r.Context(), req.Payload)
	switch {
	case err == nil:
		writeJSON(w, http.StatusCreated, job)
	case errors.Is(err, ErrQueueFull):
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "job queue is full, please retry")
	case errors.Is(err, ErrStopping):
		w.Header().Set("Retry-After", "5")
		writeError(w, http.StatusServiceUnavailable, "service is shutting down")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusRequestTimeout, "request canceled")
	default:
		writeError(w, http.StatusInternalServerError, "internal server error")
	}
}

func handleGetJob(w http.ResponseWriter, r *http.Request, service *Service) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}

	job, ok := service.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(value); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"error":"internal server error"}`+"\n")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(buf.Bytes())
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
