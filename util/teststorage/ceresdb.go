package teststorage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote/ceresdb"
)

func init() {
	go func() {
		if "true" == os.Getenv("ENABLE_PPROF") {
			addr := "localhost:6060"
			log.Printf("start pprof at %s...", addr)
			log.Println(http.ListenAndServe(addr, nil))
		}
	}()
}

func defaultEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return def
}

var (
	errTodo     = errors.New("todo")
	errNoMetric = errors.New("no metric name found")

	httpAddr = defaultEnv("CERESDB_HTTP_ADDR", "http://localhost:5440/sql")
)

// CeresDBStorage implements Storage interface
type CeresDBStorage struct {
	queryable ceresdb.Queryable
	appender  *CeresDBAppender
}

var _ storage.Storage = &CeresDBStorage{}

func NewCeresDBStorage() (*CeresDBStorage, error) {
	client, err := ceresdb.NewClient(ceresdb.GrpcAddr)
	if err != nil {
		return nil, err
	}

	w, err := ceresdb.NewWriter(client)
	if err != nil {
		return nil, err
	}
	return &CeresDBStorage{
		queryable: ceresdb.Queryable{
			Client: client,
		},
		appender: &CeresDBAppender{
			w:       w,
			metrics: make(map[string]struct{}),
		},
	}, nil
}

func (cs CeresDBStorage) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return cs.queryable.Querier(ctx, mint, maxt)
}

func (cs CeresDBStorage) ChunkQuerier(ctx context.Context, mint, maxt int64) (storage.ChunkQuerier, error) {
	return cs.queryable.ChunkQuerier(ctx, mint, maxt)
}

func (cs CeresDBStorage) Appender(ctx context.Context) storage.Appender {
	return cs.appender
}

func (cs CeresDBStorage) StartTime() (int64, error) {
	return 1, nil
}

func (cs CeresDBStorage) Close() error {
	return cs.appender.Close()
}

type CeresDBAppender struct {
	points []ceresdb.Point
	w      *ceresdb.Writer

	metrics map[string]struct{}
}

var _ storage.Appender = &CeresDBAppender{}

func (cs *CeresDBAppender) Append(ref storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	tags := make(map[string]string)
	var metric string
	for _, label := range l {
		if label.Name == labels.MetricName {
			metric = label.Value
			cs.metrics[metric] = struct{}{}
			continue
		}
		tags[label.Name] = label.Value
	}
	if metric == "" {
		return 0, errNoMetric
	}

	cs.points = append(cs.points, ceresdb.Point{
		Metric:    metric,
		Tags:      tags,
		Timestamp: t,
		Fields: map[string]float64{
			"value": v,
		},
	})
	return 0, nil
}

func (cs *CeresDBAppender) Commit() error {
	_, err := cs.w.Write(context.TODO(), cs.points)
	cs.points = cs.points[:0]
	return err
}

func (cs *CeresDBAppender) Rollback() error {
	return errTodo
}

func (cs *CeresDBAppender) AppendExemplar(ref storage.SeriesRef, l labels.Labels, e exemplar.Exemplar) (storage.SeriesRef, error) {
	return 0, errTodo
}

func (cs *CeresDBAppender) DeleteAllMetrics() error {
	if len(cs.metrics) == 0 {
		return nil
	}

	for m := range cs.metrics {
		payload := fmt.Sprintf(`{"query": "%s"}`, fmt.Sprintf("drop table `%s`", m))
		resp, err := http.Post(httpAddr, "application/json", strings.NewReader(payload))
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if ceresdb.EnableDebug {
			log.Printf("drop %s resp %s\n....", m, body)
		}
	}

	cs.metrics = make(map[string]struct{})
	return nil
}

func (cs *CeresDBAppender) Close() error {
	if err := cs.DeleteAllMetrics(); err != nil {
		return err
	}
	http.DefaultClient.CloseIdleConnections()
	return cs.w.Close()
}
