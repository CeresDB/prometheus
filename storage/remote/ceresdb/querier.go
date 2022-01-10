package ceresdb

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	"github.com/CeresDB/ceresdbproto/go/ceresdbproto"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

var errTodo = errors.New("TODO")

const (
	codeOk       = 200
	codeNotFound = 404
)

// Queryable implements SampleAndChunkQueryable interface
type Queryable struct {
	*Client
}

func NewQueryable() (*Queryable, error) {
	client, err := NewClient(GrpcAddr)
	if err != nil {
		return nil, err
	}

	return &Queryable{
		Client: client,
	}, nil
}

// Querier implements storage.Queryable interface
func (q *Queryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	// params mint&maxt is not used because:
	// Prefer SelectParams's start/end over mint/maxt here, since they are more efficient, details can be found here
	// https://github.com/prometheus/prometheus/pull/4226#issuecomment-395125769
	return &Querier{
		ctx:    ctx,
		client: q.Client,
	}, nil
}

// ChunkQuerier implements storage.ChunkQueryable interface
func (q *Queryable) ChunkQuerier(ctx context.Context, mint, maxt int64) (storage.ChunkQuerier, error) {
	return nil, errTodo
}

type Querier struct {
	ctx    context.Context
	client *Client
}

// Select implements storage.Querier interface
func (q *Querier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	return q.querySeries(hints, matchers)
}

// LabelValues implements storage.Querier interface
func (q *Querier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, errTodo
}

// LabelNames implements storage.Querier interface
func (q *Querier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, errTodo
}

// Close implements storage.Querier interface
func (q *Querier) Close() error {
	return errTodo
}

func (q *Querier) querySeries(hints *storage.SelectHints, matchers []*labels.Matcher) storage.SeriesSet {
	ctx, cancel := context.WithTimeout(q.ctx, GrpcTimeout)
	defer cancel()

	req := &ceresdbproto.PrometheusQueryRequest{
		Expr: hints.PushdownExpr,
	}
	resp, err := q.client.PromQuery(ctx, req)
	if err != nil {
		return errSeriesSet{err}
	}
	if EnableDebug {
		log.Printf("hints: %+v\n", hints)
		reqBytes, _ := json.Marshal(req)
		respBytes, _ := json.Marshal(resp)
		log.Printf("QUERY REQ:\n%s\nRESP:\n%s", reqBytes, respBytes)
	}

	switch resp.Header.Code {
	case codeNotFound:
		return &concreteSeriesSet{series: nil}
	case codeOk:
		ss := make([]storage.Series, len(resp.Timeseries))
		for i, ts := range resp.Timeseries {
			ss[i] = &ceresdbSeries{
				Metric:  hints.Metric,
				Tags:    ts.GetLabels(),
				Samples: ts.GetSamples(),
			}
		}

		return &concreteSeriesSet{series: ss}
	}

	return errSeriesSet{errors.New(resp.Header.Error)}
}
