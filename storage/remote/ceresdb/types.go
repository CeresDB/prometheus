package ceresdb

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/CeresDB/ceresdbproto/go/ceresdbproto"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// ceresdbSeries implements storage.Series.
type ceresdbSeries struct {
	// ceresdb doesn't save Metric in __name__ label
	Metric  string
	Tags    []*ceresdbproto.Label
	Samples []*ceresdbproto.Sample
}

func (c *ceresdbSeries) String() string {
	var buf strings.Builder
	buf.WriteString("metric:" + c.Metric)
	bs, _ := json.Marshal(c.Tags)
	buf.WriteString(", labels:" + string(bs))
	bs, _ = json.Marshal(c.Samples)
	buf.WriteString(", samples:" + string(bs))
	return buf.String()
}

// Labels implements storage.Labels interface
func (c *ceresdbSeries) Labels() labels.Labels {
	ret := make(labels.Labels, 0, len(c.Tags)+1)
	ret = append(ret, labels.Label{
		Name:  labels.MetricName,
		Value: c.Metric,
	})

	for _, l := range c.Tags {
		ret = append(ret, labels.Label{
			Name:  l.GetName(),
			Value: l.GetValue(),
		})
	}
	sort.Sort(ret)
	return ret
}

// Iterator implements storage.ChunkIterable
func (c *ceresdbSeries) Iterator() chunkenc.Iterator {
	return newCeresdbSeriersIterator(c)
}

// ceresdbSeriesIterator implements storage.SeriesIterator.
type ceresdbSeriesIterator struct {
	cur    int
	series *ceresdbSeries
}

func newCeresdbSeriersIterator(series *ceresdbSeries) chunkenc.Iterator {
	return &ceresdbSeriesIterator{
		cur:    -1,
		series: series,
	}
}

// Seek implements storage.SeriesIterator.
func (c *ceresdbSeriesIterator) Seek(t int64) bool {
	c.cur = sort.Search(len(c.series.Samples), func(n int) bool {
		return c.series.Samples[n].Timestamp >= t
	})
	return c.cur < len(c.series.Samples)
}

// At implements storage.SeriesIterator.
func (c *ceresdbSeriesIterator) At() (t int64, v float64) {
	s := c.series.Samples[c.cur]
	return s.Timestamp, s.Value
}

// Next implements storage.SeriesIterator.
func (c *ceresdbSeriesIterator) Next() bool {
	c.cur++
	return c.cur < len(c.series.Samples)
}

// Err implements storage.SeriesIterator.
func (c *ceresdbSeriesIterator) Err() error {
	return nil
}

// concreteSeriesSet implements storage.SeriesSet.
type concreteSeriesSet struct {
	cur    int
	series []storage.Series
}

func (c *concreteSeriesSet) Next() bool {
	c.cur++
	return c.cur-1 < len(c.series)
}

func (c *concreteSeriesSet) At() storage.Series {
	return c.series[c.cur-1]
}

func (c *concreteSeriesSet) Err() error {
	return nil
}

func (c *concreteSeriesSet) Warnings() storage.Warnings {
	return nil
}

// errSeriesSet implements storage.SeriesSet, just returning an error.
type errSeriesSet struct {
	err error
}

func (errSeriesSet) Next() bool {
	return false
}

func (errSeriesSet) At() storage.Series {
	return nil
}

func (e errSeriesSet) Err() error {
	return e.err
}

func (e errSeriesSet) Warnings() storage.Warnings { return nil }

type Point struct {
	Metric    string
	Tags      map[string]string
	Timestamp int64
	Fields    map[string]float64
}

type SingleFieldPoint struct {
	Metric    string
	Tags      map[string]string
	Timestamp int64
	Value     float64
}

func (sfp SingleFieldPoint) ToPoint() Point {
	return Point{
		Metric:    sfp.Metric,
		Tags:      sfp.Tags,
		Timestamp: sfp.Timestamp,
		Fields: map[string]float64{
			"value": sfp.Value,
		},
	}
}
