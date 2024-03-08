package dml

import (
	"context"
	"github.com/arana-db/arana/pkg/dataset"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime/ast"
)

type SortMergeJoin struct {
	Stmt *ast.SelectStatement

	LeftQuery  proto.Plan
	RightQuery proto.Plan

	JoinType ast.JoinType
	LeftKey  string
	RightKey string
}

func (h *SortMergeJoin) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (h *SortMergeJoin) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	rightDs := fetchDs(h.RightQuery, ctx, conn)
	//fields, err := rightDs.Fields()
	//
	//for true {
	//	_, err := rightDs.Next()
	//	if err != nil {
	//		if err == io.EOF {
	//			break
	//		}
	//		panic(err)
	//	}
	//}
	//if err != nil {
	//	return nil, err
	//}
	//res, err := resultx.New(resultx.WithDataset(&dataset.VirtualDataset{
	//	Columns: fields,
	//	Rows:    nil,
	//})).Dataset()
	//if err != nil {
	//	return nil, err
	//}

	//ds, err := dataset.NewSortMergeJoinDataSet(
	//	h.JoinType,
	//	&dataset.JoinKey{LeftKey: h.LeftKey, RightKey: h.RightKey},
	//	fetchDs(h.LeftQuery, ctx, conn),
	//	rightDs,
	//)
	ds, err := dataset.NewSortMergeJoinDataSet(
		h.JoinType,
		&dataset.JoinKey{LeftKey: h.RightKey, RightKey: h.LeftKey},
		rightDs,
		fetchDs(h.LeftQuery, ctx, conn),
	)
	if err != nil {
		return nil, err
	}

	return resultx.New(resultx.WithDataset(ds)), nil
}

func fetchDs(plan proto.Plan, ctx context.Context, conn proto.VConn) proto.Dataset {
	res, err := plan.ExecIn(ctx, conn)
	if err != nil {
		return nil
	}

	ds, err := res.Dataset()
	if err != nil {
		return nil
	}

	return ds
}
