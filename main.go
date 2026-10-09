package main

import (
	"context"
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/dnwe/otelsarama"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pusher/pusher-http-go/v5"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/logger"
)

func main() {
	ctx := context.Background()

	// 1. Start the logger first. It sets up tracing + log export
	//    (OTLP endpoint comes from OTEL_EXPORTER_OTLP_ENDPOINT, default localhost:4317).
	logSvc := logger.NewLoggerService("ecoop-server", "debug", "console")
	if err := logSvc.Start(ctx); err != nil {
		panic(err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = logSvc.Stop(shutdownCtx) // flushes logs and spans
	}()

	// 2. Plain logging: NewContext -> Trace -> use the returned logger.
	//    Always end the span when the unit of work is done.
	appCtx, log := logSvc.NewContext(ctx).Trace("startup")
	log.Info("booting", "env", os.Getenv("APP_ENV"))

	// 3. Database: pass the driver's connector, get back a traced *sql.DB.
	pgCfg, err := pgx.ParseConfig(os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Error(err, "parse database url")
		log.Span().End()
		return // note: log.Fatal does not exit the process, so return yourself
	}
	db := logSvc.InjectDB(stdlib.GetConnector(*pgCfg))
	defer db.Close()

	// 4. Redis: create the client as usual, then inject.
	rdb := logSvc.InjectCache(redis.NewClient(&redis.Options{Addr: os.Getenv("REDIS_ADDR")}))
	defer rdb.Close()

	// 5. Pusher: create the client as usual, then inject.
	push := logSvc.InjectPusher(&pusher.Client{
		AppID:   os.Getenv("PUSHER_APP_ID"),
		Key:     os.Getenv("PUSHER_KEY"),
		Secret:  os.Getenv("PUSHER_SECRET"),
		Cluster: os.Getenv("PUSHER_CLUSTER"),
		Secure:  true,
	})

	// 6. Kafka producer: create as usual, then inject.
	kafkaCfg := sarama.NewConfig()
	kafkaCfg.Producer.Return.Successes = true // required by SyncProducer
	rawProducer, err := sarama.NewSyncProducer([]string{os.Getenv("KAFKA_BROKER")}, kafkaCfg)
	if err != nil {
		log.Error(err, "create kafka producer")
		log.Span().End()
		return
	}
	producer := logSvc.InjectBrokerProducer(rawProducer)
	defer producer.Close()

	// 7. Kafka consumer: inject your handler before passing it to Consume.
	group, err := sarama.NewConsumerGroup([]string{os.Getenv("KAFKA_BROKER")}, "ecoop", kafkaCfg)
	if err != nil {
		log.Error(err, "create kafka consumer group")
		log.Span().End()
		return
	}
	defer group.Close()
	handler := logSvc.InjectBrokerConsumer(&userEventsHandler{logSvc: logSvc})
	go func() {
		for appCtx.Err() == nil {
			if err := group.Consume(appCtx, []string{"user-events"}, handler); err != nil {
				log.Error(err, "kafka consume")
				time.Sleep(time.Second)
			}
		}
	}()

	log.Info("dependencies ready")
	log.Span().End()

	// 8. Hertz: create the server as usual, inject BEFORE registering routes.
	h := server.Default(server.WithHostPorts(":8080"))
	logSvc.InjectHertz(h)

	h.GET("/users/:id", func(ctx context.Context, c *app.RequestContext) {
		// ctx already carries the request span; Trace adds a child span under it.
		reqCtx, log := logSvc.NewContext(ctx).Trace("GetUser",
			attribute.String("user.id", c.Param("id")),
		)
		defer log.Span().End()

		// Pass reqCtx to every call so DB/Redis spans attach to this request.
		var name string
		if err := db.QueryRowContext(reqCtx, "SELECT name FROM users WHERE id = $1", c.Param("id")).Scan(&name); err != nil {
			log.Error(err, "load user", "id", c.Param("id"))
			c.JSON(500, map[string]string{"error": "internal error"})
			return
		}
		_ = rdb.Set(reqCtx, "user:"+c.Param("id"), name, time.Minute).Err()

		// Kafka: copy the trace into the message headers so the consumer joins this trace.
		msg := &sarama.ProducerMessage{Topic: "user-events", Value: sarama.StringEncoder("viewed:" + c.Param("id"))}
		otel.GetTextMapPropagator().Inject(reqCtx, otelsarama.NewProducerMessageCarrier(msg))
		if _, _, err := producer.SendMessage(msg); err != nil {
			log.Warn("publish user event failed", "error", err)
		}

		// Pusher calls are traced too (as their own trace; pusher takes no ctx).
		_ = push.Trigger("users", "viewed", map[string]string{"id": c.Param("id")})

		log.Info("user loaded", "id", c.Param("id"))
		c.JSON(200, map[string]string{"name": name})
	})

	h.Spin()
}

// userEventsHandler is a normal sarama.ConsumerGroupHandler.
type userEventsHandler struct {
	logSvc logger.LoggerService
}

func (userEventsHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (userEventsHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (u *userEventsHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		// Pull the trace out of the message headers so logs link back to the producer.
		ctx := otel.GetTextMapPropagator().Extract(sess.Context(), otelsarama.NewConsumerMessageCarrier(msg))
		_, log := u.logSvc.NewContext(ctx).Trace("HandleUserEvent")

		log.Info("received event", "value", string(msg.Value), "offset", msg.Offset)

		log.Span().End()
		sess.MarkMessage(msg, "")
	}
	return nil
}
