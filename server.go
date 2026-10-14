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
	server := &http.Server{Handler: handler}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
	}

	started := time.Now()
	logger := zerolog.Ctx(ctx)
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
