package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type drainingListener struct{ net.Listener }

func (l drainingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		// Wait for queued response bytes before Pod networking disappears.
		if err := tcp.SetLinger(2); err != nil {
			_ = tcp.Close()
			return nil, fmt.Errorf("enable TCP response drain: %w", err)
		}
		return &drainingConn{TCPConn: tcp}, nil
	}
	return conn, nil
}

type drainingConn struct {
	*net.TCPConn
	closeOnce sync.Once
	closeErr  error
}

func (c *drainingConn) Close() error {
	// net/http can close an idle connection while its serving goroutine is
	// already closing it. Both callers must wait for the same TCP drain.
	c.closeOnce.Do(func() { c.closeErr = c.TCPConn.Close() })
	return c.closeErr
}

func serve(ctx context.Context, listener net.Listener, handler http.Handler, grace time.Duration) error {
	logger := zerolog.Ctx(ctx)
	server := &http.Server{Handler: handler}
	done := make(chan error, 1)
	go func() { done <- server.Serve(drainingListener{listener}) }()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
	}

	started := time.Now()
	logger.Info().Msg("draining active HTTP requests")
	// Do not use the canceled signal context for active requests or their drain.
	drainCtx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	if err := server.Shutdown(drainCtx); err != nil {
		_ = server.Close()
		<-done
		return fmt.Errorf("HTTP drain exceeded shutdown budget: %w", err)
	}
	if err := <-done; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	logger.Info().Dur("duration", time.Since(started)).Msg("HTTP drain completed")
	return nil
}
