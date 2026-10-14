package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

type observedListener struct {
	net.Listener
	closed chan struct{}
	once   sync.Once
}

func (l *observedListener) Close() error {
	err := l.Listener.Close()
	l.once.Do(func() { close(l.closed) })
	return err
}

func testListener(t *testing.T) *observedListener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return &observedListener{Listener: l, closed: make(chan struct{})}
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for server lifecycle")
		var zero T
		return zero
	}
}

func TestShutdownDrainsActiveResponse(t *testing.T) {
	l := testListener(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	finish := func() { releaseOnce.Do(func() { close(release) }) }
	defer finish()
	body := strings.Repeat("response", 32768)
	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(started)
			<-release
			_, _ = io.WriteString(w, body)
		}), 2*time.Second)
	}()
	response := make(chan string, 1)
	go func() {
		res, err := (&http.Client{Timeout: 3 * time.Second}).Get("http://" + l.Addr().String())
		if err != nil {
			response <- err.Error()
			return
		}
		defer res.Body.Close()
		got, err := io.ReadAll(res.Body)
		if err != nil {
			response <- err.Error()
			return
		}
		response <- string(got)
	}()
	receive(t, started)
	cancel()
	receive(t, l.closed)
	select {
	case err := <-done:
		t.Fatalf("server exited before active response completed: %v", err)
	default:
	}
	if conn, err := net.DialTimeout("tcp", l.Addr().String(), time.Second); err == nil {
		conn.Close()
		t.Fatal("server accepted a new connection while draining")
	}
	finish()
	if got := receive(t, response); got != body {
		t.Fatalf("response was interrupted: %q", got)
	}
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownBoundsHungRequest(t *testing.T) {
	l := testListener(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, canceled := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(started)
			<-r.Context().Done()
			close(canceled)
		}), 50*time.Millisecond)
	}()
	response := make(chan error, 1)
	go func() {
		res, err := (&http.Client{Timeout: 3 * time.Second}).Get("http://" + l.Addr().String())
		if res != nil {
			res.Body.Close()
		}
		response <- err
	}()
	receive(t, started)
	cancel()
	if err := receive(t, done); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected drain deadline, got %v", err)
	}
	receive(t, canceled)
	if err := receive(t, response); err == nil {
		t.Fatal("hung request unexpectedly completed")
	}
}
