package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/client"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broadcast"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broker"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/logger"
)

func main() {
	_ = godotenv.Load() // loads .env when running locally; no-op in Docker
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run() (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Start the logger first. Every step below returns its error instead of hiding it.
	//    The OTLP endpoint comes from OTEL_EXPORTER_OTLP_ENDPOINT (see .env).
	logSvc := logger.NewLoggerService(os.Getenv("APP_NAME"), os.Getenv("LOG_LEVEL"), os.Getenv("LOG_FORMAT"))
	if err := logSvc.Start(ctx); err != nil {
		return fmt.Errorf("start logger: %w", err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		err = errors.Join(err, logSvc.Stop(stopCtx)) // flushes logs, spans and metrics
	}()

	// 2. Logging: NewContext -> Trace -> log. Every log line is also an event on the span,
	//    so it shows up inside the trace in Tempo. Always end the span.
	startCtx, log := logSvc.NewContext(ctx).Trace("startup")
	defer log.Span().End()
	log.Info("booting", "env", os.Getenv("APP_ENV"))

	// Fatal = Error + immediate flush of all telemetry. It does not exit; return the error yourself.
	fatal := func(err error, msg string) error {
		log.Fatal(err, msg)
		return fmt.Errorf("%s: %w", msg, err)
	}

	// 3. Database (bun): open each pool as usual, then inject with its role.
	//    Writes go to the writer, reads to the reader (same Postgres locally).
	writer, err := openBun(os.Getenv("DATABASE_URL"))
	if err != nil {
		return fatal(err, "open writer database")
	}
	defer writer.Close()
	reader, err := openBun(os.Getenv("DATABASE_READER_URL"))
	if err != nil {
		return fatal(err, "open reader database")
	}
	defer reader.Close()
	if err := logSvc.InjectDB(writer, "writer"); err != nil {
		return fatal(err, "instrument writer database")
	}
	if err := logSvc.InjectDB(reader, "reader"); err != nil {
		return fatal(err, "instrument reader database")
	}
	if err := errors.Join(writer.PingContext(startCtx), reader.PingContext(startCtx)); err != nil {
		return fatal(err, "connect database")
	}
	log.Event("database ready")

	// 4. Redis: works for a single node, Cluster (several addrs) or Sentinel (MasterName set).
	rdb := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{os.Getenv("REDIS_ADDR")}})
	defer rdb.Close()
	if err := logSvc.InjectCache(rdb); err != nil {
		return fatal(err, "instrument redis")
	}
	log.Event("redis ready")

	// 5. Broadcaster (Sockudo locally): takes the logger interface and instruments itself.
	//    Run checks the credentials against the server and starts the delivery workers.
	broadcaster := broadcast.NewBroadcastService(
		os.Getenv("PUSHER_APP_ID"), os.Getenv("PUSHER_KEY"), os.Getenv("PUSHER_SECRET"),
		os.Getenv("PUSHER_HOST"), "", logSvc, // host already includes the port, e.g. localhost:6001
		broadcast.WithSecure(os.Getenv("PUSHER_SECURE") == "true"),
	)
	if err := broadcaster.Run(startCtx); err != nil {
		return fatal(err, "start broadcaster")
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		err = errors.Join(err, broadcaster.Stop(stopCtx)) // delivers queued broadcasts first
	}()

	// 6. Outgoing HTTP: net/http client and Hertz client. Both forward the trace to the callee.
	httpClient := &http.Client{Timeout: 5 * time.Second}
	if err := logSvc.InjectHTTPClient(httpClient); err != nil {
		return fatal(err, "instrument http client")
	}
	// The standard dialer is what makes https:// work for the Hertz client.
	hertzClient, err := client.NewClient(client.WithDialer(standard.NewDialer()), client.WithTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	if err != nil {
		return fatal(err, "create hertz client")
	}
	if err := logSvc.InjectHertzClient(hertzClient); err != nil {
		return fatal(err, "instrument hertz client")
	}

	// 7. Kafka: the broker service takes the logger interface and instruments itself.
	//    Subscribe before Run (or after; it starts right away). Failing handlers are
	//    retried, then sent to "<topic>.dlq".
	kafka := broker.NewBrokerService([]string{os.Getenv("KAFKA_BROKER")}, os.Getenv("APP_NAME"), logSvc)
	if err := kafka.Subscribe(ctx, "user-events", func(ctx context.Context, key, value []byte) error {
		// ctx carries the message's trace (continued from the publisher's request).
		_, log := logSvc.NewContext(ctx).Trace("HandleUserEvent")
		defer log.Span().End()
		if len(value) == 0 {
			return errors.New("empty user event")
		}
		log.Info("user event", "user", string(key), "action", string(value))
		return nil
	}); err != nil {
		return fatal(err, "subscribe user-events")
	}
	if err := kafka.Run(startCtx); err != nil {
		return fatal(err, "start kafka")
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		err = errors.Join(err, kafka.Stop(stopCtx))
	}()

	// 8. gRPC: instrumentation goes in at construction. Here a health server + a client calling it.
	grpcAddr := ":" + os.Getenv("GRPC_PORT")
	grpcServer := grpc.NewServer(logSvc.GRPCServerOptions()...)
	healthpb.RegisterHealthServer(grpcServer, health.NewServer())
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return fatal(err, "listen grpc")
	}
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.GracefulStop()

	grpcConn, err := grpc.NewClient("localhost"+grpcAddr,
		append(logSvc.GRPCClientOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		return fatal(err, "create grpc client")
	}
	defer grpcConn.Close()
	healthClient := healthpb.NewHealthClient(grpcConn)

	// 9. Custom metrics: create instruments once, record them anywhere.
	userLookups, err := logSvc.Meter().Int64Counter("ecoop.user.lookups",
		metric.WithDescription("User lookups by result."),
	)
	if err != nil {
		return fatal(err, "create metric")
	}

	// 10. Background jobs: each run is its own trace with duration/outcome metrics; panics become errors.
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			err := logSvc.Job(ctx, "job.count-users", func(jobCtx logger.LogContextService, log logger.LoggerLevel) error {
				n, err := reader.NewSelect().Model((*User)(nil)).Count(jobCtx)
				if err != nil {
					return fmt.Errorf("count users: %w", err)
				}
				log.Info("counted users", "count", n)
				return nil
			})
			if err != nil && !errors.Is(err, context.Canceled) {
				fmt.Fprintln(os.Stderr, "job:", err) // already logged + traced by Job
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	log.Success("dependencies ready")
	log.Span().End()

	// 11. Hertz: HTTPS when TLS_CERT_FILE/TLS_KEY_FILE are set (TLS needs the standard transport).
	opts := []config.Option{server.WithHostPorts(":" + os.Getenv("APP_PORT"))}
	if cert, key := os.Getenv("TLS_CERT_FILE"), os.Getenv("TLS_KEY_FILE"); cert != "" && key != "" {
		pair, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return fatal(err, "load tls certificate")
		}
		opts = append(opts,
			server.WithTransport(standard.NewTransporter),
			server.WithTLS(&tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}),
		)
	}
	h := server.Default(opts...)
	// Inject BEFORE registering routes.
	if err := logSvc.InjectHertz(h); err != nil {
		return fatal(err, "instrument hertz")
	}

	h.GET("/users/:id", func(ctx context.Context, c *app.RequestContext) {
		reqCtx, log := logSvc.NewContext(ctx).Trace("GetUser", attribute.String("user.id", c.Param("id")))
		defer log.Span().End()

		var user User
		err := reader.NewSelect().Model(&user).Where("id = ?", c.Param("id")).Scan(reqCtx)
		if errors.Is(err, sql.ErrNoRows) {
			userLookups.Add(reqCtx, 1, metric.WithAttributes(attribute.String("result", "not_found")))
			log.Warn("user not found", "id", c.Param("id"))
			c.JSON(404, map[string]string{"error": "user not found"})
			return
		}
		if err != nil {
			log.Error(err, "load user", "id", c.Param("id"))
			c.JSON(500, map[string]string{"error": "internal error"})
			return
		}
		userLookups.Add(reqCtx, 1, metric.WithAttributes(attribute.String("result", "found")))
		log.Event("user loaded from db")

		if err := rdb.Set(reqCtx, "user:"+c.Param("id"), user.Name, time.Minute).Err(); err != nil {
			log.Warn("cache user failed", "error", err)
		}

		if err := kafka.Publish(reqCtx, "user-events", []byte(c.Param("id")), []byte("viewed")); err != nil {
			log.Warn("publish user event failed", "error", err)
		}

		// Broadcaster: Publish waits for delivery; Broadcast queues and returns immediately.
		// Both join this request's trace.
		if err := broadcaster.Publish(reqCtx, "users", "viewed", map[string]string{"id": c.Param("id")}); err != nil {
			log.Warn("broadcast publish failed", "error", err)
		}
		if err := broadcaster.Broadcast(reqCtx, []broadcast.Channel{broadcast.Channel("user-" + c.Param("id"))}, broadcast.Events{"profile.viewed"}, user); err != nil {
			log.Warn("broadcast queue failed", "error", err)
		}

		log.Success("user loaded", "id", c.Param("id"))
		c.JSON(200, user)
	})

	h.POST("/users", func(ctx context.Context, c *app.RequestContext) {
		reqCtx, log := logSvc.NewContext(ctx).Trace("CreateUser")
		defer log.Span().End()

		var user User
		if err := c.BindJSON(&user); err != nil || user.Name == "" {
			log.Warn("invalid create user body", "error", err)
			c.JSON(400, map[string]string{"error": "name is required"})
			return
		}
		user.ID = 0
		if _, err := writer.NewInsert().Model(&user).Returning("*").Exec(reqCtx); err != nil {
			log.Error(err, "insert user")
			c.JSON(500, map[string]string{"error": "internal error"})
			return
		}
		log.Success("user created", "id", user.ID)
		c.JSON(201, user)
	})

	h.GET("/health", func(ctx context.Context, c *app.RequestContext) {
		reqCtx, log := logSvc.NewContext(ctx).Trace("Health")
		defer log.Span().End()

		checks := map[string]string{}

		if resp, err := healthClient.Check(reqCtx, &healthpb.HealthCheckRequest{}); err != nil {
			checks["grpc"] = err.Error()
		} else {
			checks["grpc"] = resp.GetStatus().String()
		}

		req, _ := http.NewRequestWithContext(reqCtx, http.MethodGet,
			"http://"+os.Getenv("PUSHER_HOST")+"/up/"+os.Getenv("PUSHER_APP_ID"), nil)
		if resp, err := httpClient.Do(req); err != nil {
			checks["sockudo"] = err.Error()
		} else {
			resp.Body.Close()
			checks["sockudo"] = resp.Status
		}

		if status, _, err := hertzClient.Get(reqCtx, nil, "https://www.google.com/generate_204"); err != nil {
			checks["internet"] = err.Error()
		} else {
			checks["internet"] = fmt.Sprint(status)
		}

		log.Success("health checked", "grpc", checks["grpc"], "sockudo", checks["sockudo"], "internet", checks["internet"])
		c.JSON(200, checks)
	})

	h.GET("/panic", func(ctx context.Context, c *app.RequestContext) {
		panic("demo panic: shows up as a failed span with stack trace")
	})

	h.Spin() // blocks until SIGINT/SIGTERM
	return nil
}

// User is the bun model for the sample users table (docker/postgres/init.sql).
type User struct {
	bun.BaseModel `bun:"table:users"`

	ID   int64  `bun:"id,pk,autoincrement" json:"id"`
	Name string `bun:"name,notnull" json:"name"`
}

// openBun opens a bun DB on the pgx driver. Instrumentation is added separately with InjectDB.
func openBun(dsn string) (*bun.DB, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	return bun.NewDB(sql.OpenDB(stdlib.GetConnector(*cfg)), pgdialect.New()), nil
}
