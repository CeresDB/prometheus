package parser

import (
	"log"

	"github.com/CeresDB/ceresdbproto/go/ceresdbproto"
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
	Expr       *ceresdbproto.Expr
	IsPushdown bool
}

func pushdownedResult(expr *ceresdbproto.Expr) TranslatedResult {
	return TranslatedResult{
		Expr:       expr,
		IsPushdown: true,
	}
}

func defaultResult(expr *ceresdbproto.Expr) TranslatedResult {
	return TranslatedResult{
		Expr: expr,
	}
}

// PushdownTranslator is used for processing expr pushdown, Expr that supports pushdown should
// implement this interface
type PushdownTranslator interface {
	Translate(*ceresdbproto.Expr, *[]storage.Series) TranslatedResult
}

var (
	_ PushdownTranslator = (*AggregateExpr)(nil)
	_ PushdownTranslator = (*Call)(nil)
	_ PushdownTranslator = (Expressions)(nil)
	_ PushdownTranslator = (*ParenExpr)(nil)
	_ PushdownTranslator = (*MatrixSelector)(nil)
)

func (_e Expressions) Translate(baseExpr *ceresdbproto.Expr, s *[]storage.Series) TranslatedResult {
	return pushdownedResult(baseExpr)
}

func (e *ParenExpr) Translate(baseExpr *ceresdbproto.Expr, s *[]storage.Series) TranslatedResult {
	return pushdownedResult(baseExpr)
}

func (e *MatrixSelector) Translate(baseExpr *ceresdbproto.Expr, s *[]storage.Series) TranslatedResult {
	return pushdownedResult(baseExpr)
}

func (e *Call) Translate(baseExpr *ceresdbproto.Expr, s *[]storage.Series) TranslatedResult {
	if !pushdownFuncs[e.Func.Name] {
		return defaultResult(baseExpr)
	}

	if ceresdb.EnableDebug {
		log.Printf("call expr: %+v, args:%v", e, e.Args)
	}
	subExpr := ceresdbproto.SubExpr{
		OpType:   ceresdbproto.SubExpr_FUNC,
		Operator: e.Func.Name,
		Operands: make([]*ceresdbproto.Expr, len(e.Args)),
	}

	subExpr.Operands[0] = baseExpr
	isPushdown := true
	for i, param := range e.Args[1:] {
		switch v := param.(type) {
		case *NumberLiteral:
			subExpr.Operands[i+1] = &ceresdbproto.Expr{
				Node: &ceresdbproto.Expr_Operand{
					Operand: &ceresdbproto.Operand{
						Value: &ceresdbproto.Operand_FloatVal{
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

	return pushdownedResult(&ceresdbproto.Expr{
		Node: &ceresdbproto.Expr_SubExpr{
			SubExpr: &subExpr,
		},
	})
}

func (e *AggregateExpr) Translate(baseExpr *ceresdbproto.Expr, s *[]storage.Series) TranslatedResult {
	if !pushdownFuncs[e.Op.String()] {
		return defaultResult(baseExpr)
	}

	if ceresdb.EnableDebug {
		log.Printf("call expr: %+v, args:%v", e, e.Expr)
	}
	subExpr := ceresdbproto.SubExpr{
		OpType:   ceresdbproto.SubExpr_AGGR,
		Operator: e.Op.String(),
		Group:    e.Grouping,
		Without:  e.Without,
	}

	isPushdown := true
	switch v := e.Param.(type) {
	case *NumberLiteral:
		subExpr.Operands = []*ceresdbproto.Expr{
			baseExpr,
			{
				Node: &ceresdbproto.Expr_Operand{
					Operand: &ceresdbproto.Operand{
						Value: &ceresdbproto.Operand_FloatVal{
							FloatVal: v.Val,
						},
					},
				},
			},
		}
	case *StringLiteral:
		subExpr.Operands = []*ceresdbproto.Expr{
			baseExpr,
			{
				Node: &ceresdbproto.Expr_Operand{
					Operand: &ceresdbproto.Operand{
						Value: &ceresdbproto.Operand_StringVal{
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
		subExpr.Operands = []*ceresdbproto.Expr{
			baseExpr,
		}
	}
	if !isPushdown {
		return defaultResult(baseExpr)
	}

	e.Pushdown = true
	e.Series = s

	return pushdownedResult(&ceresdbproto.Expr{
		Node: &ceresdbproto.Expr_SubExpr{
			SubExpr: &subExpr,
		},
	})
}
