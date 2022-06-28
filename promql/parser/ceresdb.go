package parser

import (
	"github.com/CeresDB/ceresdbproto/pkg/ceresprompb"
	"log"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote/ceresdb"
)

var (
	pushdownFuncs = map[string]bool{
		// aggregate
		"sum":   true,
		"max":   true,
		"min":   true,
		"count": true,
		"avg":   true,
		// func
		"rate":     true,
		"irate":    true,
		"delta":    true,
		"idelta":   true,
		"increase": true,
	}
)

type TranslatedResult struct {
	Expr       *ceresprompb.Expr
	IsPushdown bool
}

func pushdownedResult(expr *ceresprompb.Expr) TranslatedResult {
	return TranslatedResult{
		Expr:       expr,
		IsPushdown: true,
	}
}

func defaultResult(expr *ceresprompb.Expr) TranslatedResult {
	return TranslatedResult{
		Expr: expr,
	}
}

// PushdownTranslator is used for processing expr pushdown, Expr that supports pushdown should
// implement this interface
type PushdownTranslator interface {
	Translate(*ceresprompb.Expr, *[]storage.Series) TranslatedResult
}

var (
	_ PushdownTranslator = (*AggregateExpr)(nil)
	_ PushdownTranslator = (*Call)(nil)
	_ PushdownTranslator = (Expressions)(nil)
	_ PushdownTranslator = (*ParenExpr)(nil)
	_ PushdownTranslator = (*MatrixSelector)(nil)
)

func (_e Expressions) Translate(baseExpr *ceresprompb.Expr, s *[]storage.Series) TranslatedResult {
	return pushdownedResult(baseExpr)
}

func (e *ParenExpr) Translate(baseExpr *ceresprompb.Expr, s *[]storage.Series) TranslatedResult {
	return pushdownedResult(baseExpr)
}

func (e *MatrixSelector) Translate(baseExpr *ceresprompb.Expr, s *[]storage.Series) TranslatedResult {
	return pushdownedResult(baseExpr)
}

func (e *Call) Translate(baseExpr *ceresprompb.Expr, s *[]storage.Series) TranslatedResult {
	if !pushdownFuncs[e.Func.Name] {
		return defaultResult(baseExpr)
	}

	if ceresdb.EnableDebug {
		log.Printf("call expr: %+v, args:%v", e, e.Args)
	}
	subExpr := ceresprompb.SubExpr{
		OpType:   ceresprompb.SubExpr_FUNC,
		Operator: e.Func.Name,
		Operands: make([]*ceresprompb.Expr, len(e.Args)),
	}

	subExpr.Operands[0] = baseExpr
	isPushdown := true
	for i, param := range e.Args[1:] {
		switch v := param.(type) {
		case *NumberLiteral:
			subExpr.Operands[i+1] = &ceresprompb.Expr{
				Node: &ceresprompb.Expr_Operand{
					Operand: &ceresprompb.Operand{
						Value: &ceresprompb.Operand_FloatVal{
							FloatVal: v.Val,
						},
					},
				},
			}
			// TODO: https://github.com/prometheus/prometheus/issues/5276
		case *Call:
			isPushdown = false
		default:
			isPushdown = false
		}
	}
	if !isPushdown {
		return defaultResult(baseExpr)
	}

	e.Pushdown = true
	e.Series = s

	return pushdownedResult(&ceresprompb.Expr{
		Node: &ceresprompb.Expr_SubExpr{
			SubExpr: &subExpr,
		},
	})
}

func (e *AggregateExpr) Translate(baseExpr *ceresprompb.Expr, s *[]storage.Series) TranslatedResult {
	if !pushdownFuncs[e.Op.String()] {
		return defaultResult(baseExpr)
	}

	if ceresdb.EnableDebug {
		log.Printf("call expr: %+v, args:%v", e, e.Expr)
	}
	subExpr := ceresprompb.SubExpr{
		OpType:   ceresprompb.SubExpr_AGGR,
		Operator: e.Op.String(),
		Group:    e.Grouping,
		Without:  e.Without,
	}

	isPushdown := true
	switch v := e.Param.(type) {
	case *NumberLiteral:
		subExpr.Operands = []*ceresprompb.Expr{
			baseExpr,
			{
				Node: &ceresprompb.Expr_Operand{
					Operand: &ceresprompb.Operand{
						Value: &ceresprompb.Operand_FloatVal{
							FloatVal: v.Val,
						},
					},
				},
			},
		}
	case *StringLiteral:
		subExpr.Operands = []*ceresprompb.Expr{
			baseExpr,
			{
				Node: &ceresprompb.Expr_Operand{
					Operand: &ceresprompb.Operand{
						Value: &ceresprompb.Operand_StringVal{
							StringVal: v.Val,
						},
					},
				},
			},
		}
	case *VectorSelector:
		// https://github.com/prometheus/prometheus/issues/5276
		// not support this kinds of query
		isPushdown = false
		// TODO: https://github.com/prometheus/prometheus/issues/5276
	default:
		subExpr.Operands = []*ceresprompb.Expr{
			baseExpr,
		}
	}
	if !isPushdown {
		return defaultResult(baseExpr)
	}

	e.Pushdown = true
	e.Series = s

	return pushdownedResult(&ceresprompb.Expr{
		Node: &ceresprompb.Expr_SubExpr{
			SubExpr: &subExpr,
		},
	})
}
