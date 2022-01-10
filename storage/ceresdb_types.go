package storage

import (
	"github.com/prometheus/prometheus/model/labels"
)

type SelectType int

const (
	SelectSimple SelectType = iota
	SelectRecursive
)

type SelectExpr struct {
	Typo    SelectType
	Operand Operand
	SubExpr SelectSubExpr
}

type OperatorType int

const (
	OperatorAggr OperatorType = iota
	OperatorCall
	OperatorBinary
)

type SelectSubExpr struct {
	Typo     OperatorType
	Operator string
	Operands []SelectExpr

	// valid for aggr
	Group   []string
	Without bool

	// valid for BinaryExpr
	// If a comparison operator, return 0/1 rather than filtering.
	ReturnBool        bool
	RhsContainsMetric bool
}

type OperandType int

const (
	OperandFloat OperandType = iota
	OperandString
	OperandSelector
)

type Operand struct {
	Typo        OperandType
	FloatVal    float64
	StringVal   string
	SelectorVal Selector
}

// Note: StartMs/EndMs in Selector are used for align, not select series.
type Selector struct {
	Matchers []*labels.Matcher
	Field    string
	OffsetMs int64
	RangeMs  int64
	StartMs  int64
	EndMs    int64
}
