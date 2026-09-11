package main

import (
	"context"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestEnvString(t *testing.T) {
	if got := envString("ADDR_TEST_UNSET", defaultAddr); got != defaultAddr {
		t.Fatalf("expected the fallback, got %q", got)
	}

	t.Setenv("ADDR_TEST", ":9090")
	if got := envString("ADDR_TEST", defaultAddr); got != ":9090" {
		t.Fatalf("expected :9090, got %q", got)
	}

	t.Setenv("ADDR_TEST_EMPTY", "")
	if got := envString("ADDR_TEST_EMPTY", defaultAddr); got != defaultAddr {
		t.Fatalf("expected the fallback for an empty value, got %q", got)
	}
}

func TestEnvInt(t *testing.T) {
	cases := []struct {
		name  string
		set   bool
		value string
		want  int
	}{
		{"unset", false, "", defaultWorkers},
		{"valid", true, "12", 12},
		{"not a number", true, "many", defaultWorkers},
		{"zero", true, "0", defaultWorkers},
		{"negative", true, "-4", defaultWorkers},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key := "JOB_WORKERS_TEST"
			if tc.set {
				t.Setenv(key, tc.value)
			}
			if got := envInt(key, defaultWorkers); got != tc.want {
				t.Fatalf("expected %d, got %d", tc.want, got)
			}
		})
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func waitForServer(t *testing.T, client *http.Client, base string) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(base + "/jobs/job-unknown")
		if err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("server never became reachable")
}

func TestRunShutsDownOnContextCancel(t *testing.T) {
	addr := freeAddr(t)
	t.Setenv("ADDR", addr)
	t.Setenv("JOB_WORKERS", "2")
	t.Setenv("JOB_QUEUE_CAPACITY", "8")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx)
	}()

	client := &http.Client{Timeout: 2 * time.Second}
	base := "http://" + addr
	waitForServer(t, client, base)

	resp, err := client.Post(base+"/jobs", "application/json", strings.NewReader(`{"payload":"hello"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned an error: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("run did not return after the context was canceled")
	}

	if resp, err := client.Get(base + "/jobs/job-1"); err == nil {
		resp.Body.Close()
		t.Fatal("server is still accepting connections after shutdown")
	}
}

func TestRunReportsListenFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	t.Setenv("ADDR", listener.Addr().String())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- run(ctx)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected an error when the address is already in use")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run blocked instead of reporting the listen failure")
	}
}
