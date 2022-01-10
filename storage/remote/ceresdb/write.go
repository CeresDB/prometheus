package ceresdb

import (
	"context"
	"errors"
	"os"

	"github.com/CeresDB/ceresdbproto/go/ceresdbproto"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

type Writer struct {
	client *Client
	logger log.Logger
}

func NewWriter(c *Client) (*Writer, error) {
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stdout))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)
	return &Writer{
		client: c,
		logger: log.With(logger, "component", "ceresdb-writer"),
	}, nil
}

type column struct {
	name  string
	isTag bool
}

type columns struct {
	columns []column
}

func emptyColumns() *columns {
	return &columns{}
}

func (c *columns) add(name string, isTag bool) {
	var names []string
	if isTag {
		names = c.tagKeys()
	} else {
		names = c.fieldNames()
	}
	for _, n := range names {
		if n == name {
			return
		}
	}

	c.columns = append(c.columns, column{
		name:  name,
		isTag: isTag,
	})
}

func (c *columns) tagKeys() []string {
	var tagKeys []string
	for _, col := range c.columns {
		if col.isTag {
			tagKeys = append(tagKeys, col.name)
		}
	}
	return tagKeys
}

func (c *columns) fieldNames() []string {
	var fieldNames []string
	for _, col := range c.columns {
		if !col.isTag {
			fieldNames = append(fieldNames, col.name)
		}
	}
	return fieldNames
}

type columnValue struct {
	tags      map[int]string
	fields    map[int]float64
	timestamp int64
}

func emptyColumnValue(timestamp int64) columnValue {
	return columnValue{
		tags:      make(map[int]string),
		fields:    make(map[int]float64),
		timestamp: timestamp,
	}
}

type values struct {
	values []columnValue
}

func emptyValues() *values {
	return &values{}
}

func (vs *values) add(v columnValue) {
	vs.values = append(vs.values, v)
}

func (vs *values) toPbWriteEntry() []*ceresdbproto.WriteEntry {
	pbWriteEntries := make([]*ceresdbproto.WriteEntry, 0, len(vs.values))
	for _, cv := range vs.values {
		pbFields := make([]*ceresdbproto.Field, 0, len(cv.fields))
		for idx, field := range cv.fields {
			pbFields = append(pbFields, &ceresdbproto.Field{
				NameIndex: uint32(idx),
				Value: &ceresdbproto.Value{
					Value: &ceresdbproto.Value_Float64Value{
						Float64Value: field,
					},
				},
			})
		}
		pbTags := make([]*ceresdbproto.Tag, 0, len(cv.tags))
		for idx, tagValue := range cv.tags {
			pbTags = append(pbTags, &ceresdbproto.Tag{
				NameIndex: uint32(idx),
				Value: &ceresdbproto.Value{
					Value: &ceresdbproto.Value_StringValue{
						StringValue: tagValue,
					},
				},
			})
		}

		pbWriteEntries = append(pbWriteEntries, &ceresdbproto.WriteEntry{
			Tags: pbTags,
			FieldGroups: []*ceresdbproto.FieldGroup{
				{
					Timestamp: cv.timestamp,
					Fields:    pbFields,
				},
			},
		})
	}

	return pbWriteEntries
}

type writeRequest struct {
	points []Point
}

func (wr writeRequest) toPb() *ceresdbproto.WriteRequest {
	metricColumns := make(map[string]*columns)
	for _, p := range wr.points {
		var columns *columns
		if existsColumns, ok := metricColumns[p.Metric]; ok {
			columns = existsColumns
		} else {
			columns = emptyColumns()
			metricColumns[p.Metric] = columns
		}

		for tagKey := range p.Tags {
			columns.add(tagKey, true)
		}
		for fieldName := range p.Fields {
			columns.add(fieldName, false)
		}
	}

	metricValues := make(map[string]*values, len(metricColumns))
	for _, p := range wr.points {
		columns := metricColumns[p.Metric]
		var values *values
		if v, ok := metricValues[p.Metric]; ok {
			values = v
		} else {
			values = emptyValues()
			metricValues[p.Metric] = values
		}

		cv := emptyColumnValue(p.Timestamp)
		tagIdx, fieldIdx := 0, 0
		for _, col := range columns.columns {
			if col.isTag {
				cv.tags[tagIdx] = p.Tags[col.name]
				tagIdx++
			} else {
				cv.fields[fieldIdx] = p.Fields[col.name]
				fieldIdx++
			}
		}
		values.add(cv)

	}

	pbRequest := &ceresdbproto.WriteRequest{}
	for metric, columns := range metricColumns {
		wm := &ceresdbproto.WriteMetric{
			Metric:     metric,
			TagNames:   columns.tagKeys(),
			FieldNames: columns.fieldNames(),
			Entries:    metricValues[metric].toPbWriteEntry(),
		}
		pbRequest.Metrics = append(pbRequest.Metrics, wm)
	}

	return pbRequest
}

func (w *Writer) Write(ctx context.Context, points []Point) (int, error) {
	req := writeRequest{
		points: points,
	}

	resp, err := w.client.Write(ctx, req.toPb())
	if err != nil {
		return 0, err
	}

	if codeOk == resp.Header.Code {
		return int(resp.Success), nil
	}

	level.Error(w.logger).Log("error", resp.Header, "success", resp.Success, "failed", resp.Failed)
	return 0, errors.New(resp.Header.GetError())
}

func (w *Writer) Close() error {
	return w.client.Close()
}
