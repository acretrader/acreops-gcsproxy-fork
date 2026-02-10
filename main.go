package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/gorilla/mux"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/api/option"
)

var (
	bind         = flag.String("b", "127.0.0.1:8080", "Bind address")
	verbose      = flag.Bool("v", false, "Enable debug logging")
	credentials  = flag.String("c", "", "The path to the keyfile. If not present, client will use your default application credentials.")
	defaultIndex = flag.String("i", "", "The default index file to serve.")
)

var client *storage.Client

func handleError(w http.ResponseWriter, r *http.Request, err error) {
	logger := zerolog.Ctx(r.Context())
	span := trace.SpanFromContext(r.Context())

	if errors.Is(err, storage.ErrObjectNotExist) {
		logger.Warn().Err(err).Msg("object not found")
		http.Error(w, err.Error(), http.StatusNotFound)
	} else {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		logger.Error().Err(err).Msg("internal error")
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func header(r *http.Request, key string) (string, bool) {
	if r.Header == nil {
		return "", false
	}
	if candidate := r.Header[key]; len(candidate) > 0 {
		return candidate[0], true
	}
	return "", false
}

func setStrHeader(w http.ResponseWriter, key string, value string) {
	if value != "" {
		w.Header().Add(key, value)
	}
}

func setIntHeader(w http.ResponseWriter, key string, value int64) {
	if value > 0 {
		w.Header().Add(key, strconv.FormatInt(value, 10))
	}
}

func setTimeHeader(w http.ResponseWriter, key string, value time.Time) {
	if !value.IsZero() {
		w.Header().Add(key, value.UTC().Format(http.TimeFormat))
	}
}

type wrapResponseWriter struct {
	http.ResponseWriter
	status       int
	bytesWritten int64
}

func (w *wrapResponseWriter) WriteHeader(status int) {
	w.ResponseWriter.WriteHeader(status)
	w.status = status
}

func (w *wrapResponseWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.bytesWritten += int64(n)
	return n, err
}

func wrapper(fn func(w http.ResponseWriter, r *http.Request), baseLogger zerolog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		proc := time.Now()
		writer := &wrapResponseWriter{
			ResponseWriter: w,
			status:         http.StatusOK,
		}

		// Extract Acres headers
		acresSessionID := r.Header.Get("X-Acres-Session-Id")
		acresRequestID := r.Header.Get("X-Acres-Request-Id")
		rangeHeader := r.Header.Get("Range")
		ifRange := r.Header.Get("If-Range")

		// Set span attributes (otelmux already started the span)
		span := trace.SpanFromContext(r.Context())
		if acresSessionID != "" {
			span.SetAttributes(attribute.String("acres.session_id", acresSessionID))
		}
		if acresRequestID != "" {
			span.SetAttributes(attribute.String("acres.request_id", acresRequestID))
		}
		if rangeHeader != "" {
			span.SetAttributes(attribute.String("http.request.header.range", rangeHeader))
		}
		if ifRange != "" {
			span.SetAttributes(attribute.String("http.request.header.if_range", ifRange))
		}

		// Build per-request logger with trace context
		traceID, spanID := extractTraceContext(r.Context())
		reqLogger := baseLogger.With().
			Str("trace_id", traceID).
			Str("span_id", spanID).
			Str("acres_session_id", acresSessionID).
			Str("acres_request_id", acresRequestID).
			Logger()
		r = r.WithContext(reqLogger.WithContext(r.Context()))

		// Panic recovery
		defer func() {
			if rec := recover(); rec != nil {
				span.SetStatus(codes.Error, fmt.Sprintf("panic: %v", rec))
				span.RecordError(fmt.Errorf("panic: %v", rec))
				panicCounter.Add(r.Context(), 1)

				reqLogger.Error().
					Interface("panic", rec).
					Str("stack", string(debug.Stack())).
					Msg("panic recovered")

				http.Error(writer, "Internal Server Error", http.StatusInternalServerError)
				writer.status = http.StatusInternalServerError
			}

			// Access log + metrics (runs after handler and after panic recovery)
			duration := time.Since(proc)
			addr := r.RemoteAddr
			if ip, found := header(r, "X-Forwarded-For"); found {
				addr = ip
			}

			// Metrics
			bytesServed.Add(r.Context(), writer.bytesWritten)
			if rangeHeader != "" {
				rangeType := "full_fallback"
				if writer.status == http.StatusPartialContent {
					rangeType = "partial"
				}
				rangeRequests.Add(r.Context(), 1,
					metric.WithAttributes(attribute.String("type", rangeType)),
				)
			}

			// Access log — health checks at debug level, everything else at info
			logEvent := reqLogger.Info()
			if r.URL.Path == "/_health" {
				logEvent = reqLogger.Debug()
			}
			logEvent.
				Str("client_ip", addr).
				Str("method", r.Method).
				Str("path", r.URL.String()).
				Int("status", writer.status).
				Float64("duration_s", duration.Seconds()).
				Int64("bytes", writer.bytesWritten).
				Str("range", rangeHeader).
				Str("user_agent", r.UserAgent()).
				Msg("request")
		}()

		fn(writer, r)
	}
}

func fetchObjectAttrs(ctx context.Context, bucket, object string) (*storage.ObjectAttrs, error) {
	attrs, err := client.Bucket(bucket).Object(strings.TrimSuffix(object, "/")).Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			if *defaultIndex == "" {
				return nil, err
			}
			object, err := url.JoinPath(object, *defaultIndex)
			if err != nil {
				return nil, err
			}
			return client.Bucket(bucket).Object(object).Attrs(ctx)
		}
		return nil, err
	}
	return attrs, nil
}

func proxy(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	params := mux.Vars(r)
	logger := zerolog.Ctx(ctx)

	// Fetch GCS object attributes
	ctx, attrSpan := tracer.Start(ctx, "fetchObjectAttrs",
		trace.WithAttributes(
			attribute.String("gcs.bucket", params["bucket"]),
			attribute.String("gcs.object", params["object"]),
		))
	attrs, err := fetchObjectAttrs(ctx, params["bucket"], params["object"])
	attrSpan.End()

	if err != nil {
		handleError(w, r, err)
		return
	}

	if lastStrs, ok := r.Header["If-Modified-Since"]; ok && len(lastStrs) > 0 {
		last, err := http.ParseTime(lastStrs[0])
		if err != nil {
			logger.Debug().Err(err).Msg("could not parse If-Modified-Since")
		}
		if !attrs.Updated.Truncate(time.Second).After(last) {
			w.WriteHeader(304)
			return
		}
	}

	setTimeHeader(w, "Last-Modified", attrs.Updated)
	setStrHeader(w, "Content-Type", attrs.ContentType)
	setStrHeader(w, "Content-Language", attrs.ContentLanguage)
	setStrHeader(w, "Cache-Control", attrs.CacheControl)
	setStrHeader(w, "Content-Disposition", attrs.ContentDisposition)

	gzipAcceptable := strings.Contains(r.Header.Get("Accept-Encoding"), "gzip")

	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
		objH := client.Bucket(attrs.Bucket).Object(attrs.Name).ReadCompressed(gzipAcceptable)
		serveRange(w, r, attrs.Updated, attrs.Size, objH)
		return
	}

	// Full content read
	ctx, readerSpan := tracer.Start(ctx, "gcs.NewReader")
	objr, err := client.Bucket(attrs.Bucket).Object(attrs.Name).ReadCompressed(gzipAcceptable).NewReader(ctx)
	readerSpan.End()

	if err != nil {
		handleError(w, r, err)
		return
	}
	defer objr.Close()

	setStrHeader(w, "Content-Encoding", objr.Attrs.ContentEncoding)
	setIntHeader(w, "Content-Length", objr.Attrs.Size)
	io.Copy(w, objr)
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	setStrHeader(w, "Content-Type", "text/plain")
	io.WriteString(w, "OK\n")
}

func main() {
	flag.Parse()

	// Initialize zerolog
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	baseLogger := zerolog.New(os.Stdout).With().Timestamp().Str("service", "kepler").Logger()
	if *verbose {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	// Initialize OTel
	ctx := context.Background()
	shutdown, err := initTelemetry(ctx)
	if err != nil {
		baseLogger.Fatal().Err(err).Msg("failed to initialize telemetry")
	}
	defer shutdown(ctx)

	// GCS client
	if *credentials != "" {
		client, err = storage.NewClient(ctx, option.WithCredentialsFile(*credentials))
	} else {
		client, err = storage.NewClient(ctx)
	}
	if err != nil {
		baseLogger.Fatal().Err(err).Msg("failed to create GCS client")
	}

	// Router
	r := mux.NewRouter()
	r.Use(otelmux.Middleware("kepler",
		otelmux.WithFilter(func(r *http.Request) bool {
			return r.URL.Path != "/_health"
		}),
	))
	r.HandleFunc("/_health", wrapper(healthCheck, baseLogger)).Methods("GET", "HEAD")
	r.HandleFunc("/{bucket:[0-9a-zA-Z-_.]+}/{object:.*}", wrapper(proxy, baseLogger)).Methods("GET", "HEAD")

	baseLogger.Info().Str("bind", *bind).Msg("listening")
	if err := http.ListenAndServe(*bind, r); err != nil {
		baseLogger.Fatal().Err(err).Msg("server failed")
	}
}
