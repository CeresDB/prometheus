// Copyright 2021 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package remote

import (
	"context"
	"fmt"
	stdLog "log"
	"net/http"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote/ceresdb"
)

type writeHandler struct {
	logger     log.Logger
	appendable storage.Appendable
}

// NewWriteHandler creates a http.Handler that accepts remote write requests and
// writes them to the provided appendable.
func NewWriteHandler(logger log.Logger, appendable storage.Appendable) http.Handler {
	return &writeHandler{
		logger:     logger,
		appendable: appendable,
	}
}

func (h *writeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	req, err := DecodeWriteRequest(r.Body)
	if err != nil {
		level.Error(h.logger).Log("msg", "Error decoding remote write request", "err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = h.write(r.Context(), req)
	switch err {
	case nil:
	case storage.ErrOutOfOrderSample, storage.ErrOutOfBounds, storage.ErrDuplicateSampleForTimestamp:
		// Indicated an out of order sample is a bad request to prevent retries.
		level.Error(h.logger).Log("msg", "Out of order sample from remote write", "err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	default:
		level.Error(h.logger).Log("msg", "Error appending remote write", "err", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// checkAppendExemplarError modifies the AppendExamplar's returned error based on the error cause.
func (h *writeHandler) checkAppendExemplarError(err error, e exemplar.Exemplar, outOfOrderErrs *int) error {
	switch errors.Cause(err) {
	case storage.ErrNotFound:
		return storage.ErrNotFound
	case storage.ErrOutOfOrderExemplar:
		*outOfOrderErrs++
		level.Debug(h.logger).Log("msg", "Out of order exemplar", "exemplar", fmt.Sprintf("%+v", e))
		return nil
	default:
		return err
	}
}

func (h *writeHandler) write(ctx context.Context, req *prompb.WriteRequest) (err error) {
	if ceresdb.HackRemoteWrite {
		return h.writeToCeresdb(ctx, req)
	}

	outOfOrderExemplarErrs := 0

	app := h.appendable.Appender(ctx)
	defer func() {
		if err != nil {
			_ = app.Rollback()
			return
		}
		err = app.Commit()
	}()

	var exemplarErr error
	for _, ts := range req.Timeseries {
		labels := labelProtosToLabels(ts.Labels)
		for _, s := range ts.Samples {
			_, err = app.Append(0, labels, s.Timestamp, s.Value)
			if err != nil {
				return err
			}

		}

		for _, ep := range ts.Exemplars {
			e := exemplarProtoToExemplar(ep)

			_, exemplarErr = app.AppendExemplar(0, labels, e)
			exemplarErr = h.checkAppendExemplarError(exemplarErr, e, &outOfOrderExemplarErrs)
			if exemplarErr != nil {
				// Since exemplar storage is still experimental, we don't fail the request on ingestion errors.
				level.Debug(h.logger).Log("msg", "Error while adding exemplar in AddExemplar", "exemplar", fmt.Sprintf("%+v", e), "err", exemplarErr)
			}
		}
	}

	if outOfOrderExemplarErrs > 0 {
		_ = level.Warn(h.logger).Log("msg", "Error on ingesting out-of-order exemplars", "num_dropped", outOfOrderExemplarErrs)
	}

	return nil
}

// Hack by CeresDB
var ceresdbWriter *ceresdb.Writer

func init() {
	client, err := ceresdb.NewClient(ceresdb.GrpcAddr)
	if err != nil {
		stdLog.Fatalf("init ceresdb grpc client failed, err:%v", err)
	}

	writer, err := ceresdb.NewWriter(client)
	if err != nil {
		stdLog.Fatalf("init ceresdb writer failed, err:%v", err)
	}

	ceresdbWriter = writer
}

const nameLabel = "__name__"

func (h *writeHandler) writeToCeresdb(ctx context.Context, req *prompb.WriteRequest) error {
	var points []ceresdb.Point
	for _, ts := range req.Timeseries {
		labels := labelProtosToLabels(ts.Labels)
		tags := labels.Map()
		metric := tags[nameLabel]
		delete(tags, nameLabel)
		for _, s := range ts.Samples {
			points = append(points, ceresdb.Point{
				Metric:    metric,
				Tags:      tags,
				Timestamp: s.Timestamp,
				Fields: map[string]float64{
					"value": s.Value,
				},
			})
		}
	}
	if ceresdb.EnableDebug {
		stdLog.Printf("remote write to ceresdb, points:%+v\n", points)
	}

	_, err := ceresdbWriter.Write(ctx, points)
	return err
}
