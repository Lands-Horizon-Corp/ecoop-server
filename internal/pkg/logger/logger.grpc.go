package logger

import (
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

// GRPCServerOptions traces and measures every RPC a gRPC server handles and
// continues the caller's trace. Pass them when building the server:
//
//	srv := grpc.NewServer(append(log.GRPCServerOptions(), yourOpts...)...)
func (l *loggerServiceImpl) GRPCServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{grpc.StatsHandler(otelgrpc.NewServerHandler())}
}

// GRPCClientOptions traces and measures every RPC a gRPC client makes and
// forwards the trace to the server. Pass them when building the client:
//
//	conn, err := grpc.NewClient(addr, append(log.GRPCClientOptions(), yourOpts...)...)
func (l *loggerServiceImpl) GRPCClientOptions() []grpc.DialOption {
	return []grpc.DialOption{grpc.WithStatsHandler(otelgrpc.NewClientHandler())}
}
