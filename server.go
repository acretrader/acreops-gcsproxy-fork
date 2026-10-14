package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/rs/zerolog"
)

func serve(ctx context.Context, listener net.Listener, handler http.Handler, grace time.Duration) error {
	logger := zerolog.Ctx(ctx)
	server := &http.Server{
		Handler: handler,
		ConnContext: func(connCtx context.Context, conn net.Conn) context.Context {
			// A completed HTTP write may still have bytes in the kernel send queue.
			// On Linux, linger makes Close wait for delivery before Pod networking disappears.
			if tcp, ok := conn.(*net.TCPConn); ok {
				if err := tcp.SetLinger(2); err != nil {
					logger.Error().Err(err).Msg("cannot enable TCP response drain")
				}
			}
			return connCtx
		},
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()

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
