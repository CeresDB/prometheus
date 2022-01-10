package ceresdb

import (
	"log"
	"os"
	"time"
)

func getEnv(env, def string) string {
	if v := os.Getenv(env); v != "" {
		return v
	}

	return def
}

func assertf(err error, format string, v ...interface{}) {
	if err != nil {
		log.Fatalf(format, v...)
	}
}

func assert(err error) {
	assertf(err, "")
}

func mustParseDuration(dur string) time.Duration {
	d, err := time.ParseDuration(dur)
	assert(err)

	return d
}

var (
	// GrpcAddr set CeresDB gRPC address
	GrpcAddr string
	// GrpcTimeout set timeout when doing gRPC call
	GrpcTimeout time.Duration
	// EnableDebug set whether enable debug level log
	EnableDebug bool
	// HackRemoteWrite control remote write
	HackRemoteWrite bool
)

func init() {
	GrpcAddr = getEnv("CERESDB_GRPC_ADDR", ":8831")
	GrpcTimeout = mustParseDuration(getEnv("CERESDB_GRPC_TIMEOUT", "1m"))
	EnableDebug = getEnv("CERESDB_ENABLE_DEBUG", "false") == "true"
	HackRemoteWrite = getEnv("CERESDB_REMOTE_WRITE", "false") == "true"
}
