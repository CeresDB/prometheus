package v1

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/prometheus/prometheus/storage/remote/ceresdb"
)

var ceresdbWriter *ceresdb.Writer

func init() {
	client, err := ceresdb.NewClient(ceresdb.GrpcAddr)
	if err != nil {
		log.Fatalf("init ceresdb grpc client failed, err:%v", err)
	}

	writer, err := ceresdb.NewWriter(client)
	if err != nil {
		log.Fatalf("init ceresdb writer failed, err:%v", err)
	}

	ceresdbWriter = writer
}

func (api *API) ceresdbWrite(r *http.Request) apiFuncResult {
	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := parseDuration(to)
		if err != nil {
			return invalidParamError(err, "timeout")
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	bs, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return invalidParamError(err, "read http body")
	}

	var points []ceresdb.SingleFieldPoint
	if err := json.Unmarshal(bs, &points); err != nil {
		return invalidParamError(err, "invalid JSON")
	}

	ps2 := make([]ceresdb.Point, len(points))
	for i, p := range points {
		ps2[i] = p.ToPoint()
	}
	success, err := ceresdbWriter.Write(ctx, ps2)
	if err != nil {
		return apiFuncResult{nil, returnAPIError(err), nil, nil}
	}

	return apiFuncResult{success, nil, nil, nil}
}
