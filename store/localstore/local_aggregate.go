package localstore

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

// Update aggregate functions with rows.
func (rs *localRegion) aggregate(ctx *selectContext, row [][]byte) error {
	cols := ctx.sel.TableInfo.Columns
	for i, col := range cols {
		_, datum, err := codec.DecodeOne(row[i])
		if err != nil {
			return errors.Trace(err)
		}
		ctx.eval.Row[col.GetColumnId()] = datum
	}
	// TODO: Get group key
	for _, agg := range ctx.aggregates {
		// update aggregate funcs
		args := make([]types.Datum, len(agg.expr.Children))
		// Evaluate args
		for _, x := range agg.expr.Children {
			// Evaluate it
			cv, err := ctx.eval.Eval(x)
			if err != nil {
				return errors.Trace(err)
			}
			args = append(args, cv)
		}
		agg.update(ctx, args)
	}
	return nil
}

// Partial result for a single aggregate group.
type aggItem struct {
	// Number of rows, this could be used in cout/avg
	count uint64
	// This could be used to store sum/max/min
	value types.Datum
	// TODO: support group_concat
}

func (ai *aggItem) toBytes() []bytes {
	// Convert each aggItem to []bytes

	// datum to bytes
	return p
}

// This is similar to ast.AggregateFuncExpr but use tipb.Expr.
type aggregateFuncExpr struct {
	expr         *tipb.Expr
	currentGroup []byte
	// contextPerGroupMap is used to store aggregate evaluation context.
	// Each entry for a group.
	contextPerGroupMap map[string](*aggItem)
}

// Clear clears aggregate computing context.
func (n *aggregateFuncExpr) clear() {
	n.currentGroup = []byte{}
	n.contextPerGroupMap = nil
}

// Update is used for update aggregate context.
func (n *aggregateFuncExpr) update(ctx *selectContext, args []types.Datum) error {
	switch n.expr.GetTp() {
	case tipb.ExprType_Count:
		return n.updateCount(ctx, args)
	}
	return errors.New(fmt.Sprintf("Unknown AggExpr: %v", n.expr.GetTp()))
}

func (n *aggregateFuncExpr) toProto() *tipb.AggExpr {
	groups := make([]*tipb.AggGroupEntry, 0, len(n.contextPerGroupMap))
	for key, item := range n.contextPerGroupMap {
		entry := &tipb.AggGroupEntry{
			Key:  []byte(key),
			Item: item.toProto(),
		}
		groups = append(groups, entry)
	}
	return &tipb.AggExpr{Groups: groups}
}

var singleGroupKey = []byte("SingleGroup")

// getContext gets aggregate evaluation context for the current group.
// If it is nil, add a new context into contextPerGroupMap.
func (n *aggregateFuncExpr) getAggItem() *aggItem {
	if n.currentGroup == nil {
		n.currentGroup = singleGroupKey
	}
	if n.contextPerGroupMap == nil {
		n.contextPerGroupMap = make(map[string](*aggItem))
	}
	if _, ok := n.contextPerGroupMap[string(n.currentGroup)]; !ok {
		n.contextPerGroupMap[string(n.currentGroup)] = &aggItem{}
	}
	return n.contextPerGroupMap[string(n.currentGroup)]
}

func (n *aggregateFuncExpr) updateCount(ctx *selectContext, args []types.Datum) error {
	aggItem := n.getAggItem()
	aggItem.count++
	return nil
}
