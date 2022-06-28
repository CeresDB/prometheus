package promql

import (
	"log"

	"github.com/CeresDB/ceresdbproto/pkg/ceresprompb"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote/ceresdb"
)

type pushdownHelper struct {
	start       int64
	end         int64
	step        int64
	series      *[]storage.Series
	matchers    []*labels.Matcher
	alignStart  int64
	alignEnd    int64
	offset      int64
	matrixRange int64
	path        []parser.Node
}

func normalizeMatchers(matchers []*labels.Matcher) ([]*labels.Matcher, bool) {
	pushdownEnabled := true
	ms := make([]*labels.Matcher, 0, len(matchers))
	for _, m := range matchers {
		switch m.Name {
		case ceresdb.PushdownName:
			if m.Value == "false" {
				pushdownEnabled = false
			}
		default:
			ms = append(ms, m)
		}
	}
	return ms, pushdownEnabled
}

func (ph pushdownHelper) toExpr() (string, *ceresprompb.Expr, error) {
	matchers, pushdownEnabled := normalizeMatchers(ph.matchers)
	qm, err := ceresdb.QueryParamFrom(matchers)
	if err != nil {
		return "", nil, err
	}

	step := ph.step
	if step == 0 {
		step = 1
	}
	baseExpr := &ceresprompb.Expr{
		Node: &ceresprompb.Expr_Operand{
			Operand: &ceresprompb.Operand{
				Value: &ceresprompb.Operand_Selector{
					Selector: &ceresprompb.Selector{
						Measurement: qm.Metric,
						Filters:     qm.Filters,
						Start:       ph.start,
						End:         ph.end,
						Range:       ph.matrixRange,
						Offset:      ph.offset,
						Field:       qm.Field,
						AlignStart:  ph.alignStart,
						AlignEnd:    ph.alignEnd,
						Step:        step,
					},
				},
			},
		},
	}
	if !pushdownEnabled {
		return qm.Metric, baseExpr, nil
	}

	pathLen := len(ph.path)
	if pathLen > 0 {
		for i := 0; i < pathLen; i++ {
			if _, isSubquery := ph.path[i].(*parser.SubqueryExpr); isSubquery {
				// cannot pushdown when path contains a subquery
				return qm.Metric, baseExpr, nil
			}
		}
		pushdownIdx := pathLen - 1 // exclude last path
		for ; pushdownIdx >= 0; pushdownIdx-- {
			if ceresdb.EnableDebug {
				log.Printf("path[%d] is %s", pushdownIdx, ph.path[pushdownIdx])
			}
			if tr, ok := ph.path[pushdownIdx].(parser.PushdownTranslator); ok {
				ret := tr.Translate(baseExpr, ph.series)
				if ret.IsPushdown {
					baseExpr = ret.Expr
				} else {
					break
				}
			} else {
				break
			}
		}
		// For debug
		if ceresdb.EnableDebug {
			for i := len(ph.path) - 1; i >= 0; i-- {
				pd := false
				call := false
				aggr := false
				if c, ok := ph.path[i].(*parser.Call); ok {
					call = true
					pd = c.Pushdown
				} else if c, ok := ph.path[i].(*parser.AggregateExpr); ok {
					aggr = true
					pd = c.Pushdown
				}
				log.Printf("debug path[%d] is %s call:%v, aggr:%v, %v", i, ph.path[i], call, aggr, pd)
			}
		}

	}

	return qm.Metric, baseExpr, nil
}

func series2matrix(series []storage.Series) Matrix {
	matrix := make([]Series, 0, len(series))
	for _, s := range series {
		// TODO(chenxiang): reuse points
		points := make([]Point, 0, 64)
		for it := s.Iterator(); it.Next(); {
			t, v := it.At()
			points = append(points, Point{T: t, V: v})
		}
		if len(points) > 0 {
			matrix = append(matrix, Series{
				// dropMetricName for aggr/func expr
				Metric: dropMetricName(s.Labels()),
				Points: points,
			})
		}
	}
	return matrix
}
