/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

import (
	"github.com/bwmarrin/snowflake"

	perrors "github.com/pkg/errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"go.uber.org/atomic"

	"golang.org/x/sync/errgroup"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/metrics"
	"github.com/arana-db/arana/pkg/mysql"
	errors2 "github.com/arana-db/arana/pkg/mysql/errors"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/hint"
	"github.com/arana-db/arana/pkg/resultx"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	_ "github.com/arana-db/arana/pkg/runtime/function"
	"github.com/arana-db/arana/pkg/runtime/namespace"
	"github.com/arana-db/arana/pkg/runtime/optimize"
	_ "github.com/arana-db/arana/pkg/runtime/optimize/dal"
	_ "github.com/arana-db/arana/pkg/runtime/optimize/ddl"
	_ "github.com/arana-db/arana/pkg/runtime/optimize/dml"
	_ "github.com/arana-db/arana/pkg/runtime/optimize/utility"
	"github.com/arana-db/arana/pkg/util/log"
	"github.com/arana-db/arana/pkg/util/rand2"
	"github.com/arana-db/arana/third_party/pools"
)

var (
	_ Runtime     = (*defaultRuntime)(nil)
	_ proto.VConn = (*defaultRuntime)(nil)
	_ proto.VConn = (*compositeTx)(nil)
)

var Tracer = otel.Tracer("Runtime")

var errTxClosed = errors.New("transaction is closed")

// Runtime executes a sql statement.
type Runtime interface {
	proto.Executable
	proto.VConn
	// Namespace returns the namespace.
	Namespace() *namespace.Namespace
	// Begin begins a new transaction.
	Begin(ctx context.Context) (proto.Tx, error)
}

// Load loads a Runtime, here schema means logical database name.
func Load(schema string) (Runtime, error) {
	var ns *namespace.Namespace
	if ns = namespace.Load(schema); ns == nil {
		return nil, perrors.Errorf("no such logical database %s", schema)
	}
	return (*defaultRuntime)(ns), nil
}

// Unload unloads a Runtime, here schema means logical database name.
func Unload(schema string) error {
	if err := namespace.Unregister(schema); err != nil {
		return perrors.Wrapf(err, "cannot unload schema '%s'", schema)
	}
	return nil
}

var (
	_ proto.DB             = (*AtomDB)(nil)
	_ proto.Callable       = (*atomTx)(nil)
	_ proto.Tx             = (*compositeTx)(nil)
	_ proto.VersionSupport = (*compositeTx)(nil)
)

type compositeTx struct {
	closed atomic.Bool
	id     int64

	rt  *defaultRuntime
	txs map[string]*atomTx
}

func (tx *compositeTx) Version(ctx context.Context) (string, error) {
	return tx.rt.Version(ctx)
}

func (tx *compositeTx) Query(ctx context.Context, db string, query string, args ...proto.Value) (proto.Result, error) {
	return tx.call(ctx, db, query, args...)
}

func (tx *compositeTx) Exec(ctx context.Context, db string, query string, args ...proto.Value) (proto.Result, error) {
	return tx.call(ctx, db, query, args...)
}

func (tx *compositeTx) call(ctx context.Context, db string, query string, args ...proto.Value) (proto.Result, error) {
	if len(db) < 1 {
		db = tx.rt.Namespace().DBGroups()[0]
	}

	atx, err := tx.begin(ctx, db)
	if err != nil {
		return nil, err
	}

	log.Debugf("call upstream: db=%s, sql=\"%s\", args=%v", db, query, args)

	res, _, err := atx.Call(ctx, query, args...)
	if err != nil {
		return nil, perrors.WithStack(err)
	}
	return res, nil
}

func (tx *compositeTx) begin(ctx context.Context, group string) (*atomTx, error) {
	if exist, ok := tx.txs[group]; ok {
		return exist, nil
	}

	// force use writeable node
	ctx = rcontext.WithWrite(ctx)
	db := selectDB(ctx, group, tx.rt.Namespace())
	if db == nil {
		return nil, perrors.Errorf("cannot get upstream database %s", group)
	}

	// begin atom tx
	newborn, err := db.(*AtomDB).begin(ctx)
	if err != nil {
		return nil, err
	}
	tx.txs[group] = newborn
	return newborn, nil
}

func (tx *compositeTx) String() string {
	return fmt.Sprintf("tx-%d", tx.id)
}

func (tx *compositeTx) Execute(ctx *proto.Context) (res proto.Result, warn uint16, err error) {
	var span trace.Span
	ctx.Context, span = Tracer.Start(ctx.Context, "compositeTx.Execute")
	execStart := time.Now()
	defer func() {
		span.End()
		metrics.ExecuteDuration.Observe(time.Since(execStart).Seconds())
	}()
	if tx.closed.Load() {
		err = errTxClosed
		return
	}

	args := ctx.GetArgs()
	if direct := rcontext.IsDirect(ctx.Context); direct {
		var (
			group = tx.rt.Namespace().DBGroups()[0]
			atx   *atomTx
			cctx  = rcontext.WithWrite(ctx.Context)
		)
		if atx, err = tx.begin(cctx, group); err != nil {
			return
		}
		res, warn, err = atx.Call(cctx, ctx.GetQuery(), args...)
		if err != nil {
			err = perrors.WithStack(err)
		}
		return
	}

	var (
		ru   = tx.rt.Namespace().Rule()
		plan proto.Plan
	)

	ctx.Context = rcontext.WithHints(ctx.Context, ctx.Stmt.Hints)

	var opt proto.Optimizer
	if opt, err = optimize.NewOptimizer(ru, ctx.Stmt.Hints, ctx.Stmt.StmtNode, args); err != nil {
		err = perrors.WithStack(err)
		return
	}

	if plan, err = opt.Optimize(ctx); err != nil {
		err = perrors.WithStack(err)
		return
	}

	if res, err = plan.ExecIn(ctx, tx); err != nil {
		// TODO: how to warp error packet
		err = perrors.WithStack(err)
		return
	}

	return
}

func (tx *compositeTx) ID() int64 {
	return tx.id
}

func (tx *compositeTx) Commit(ctx context.Context) (proto.Result, uint16, error) {
	if !tx.closed.CAS(false, true) {
		return nil, 0, errTxClosed
	}
	ctx, span := Tracer.Start(ctx, "compositeTx.Commit")
	defer func() { // cleanup
		tx.rt = nil
		tx.txs = nil
		span.End()
	}()

	var g errgroup.Group
	for k, v := range tx.txs {
		k, v := k, v
		g.Go(func() error {
			_, _, err := v.Commit(ctx)
			if err != nil {
				log.Errorf("commit %s for group %s failed: %v", tx, k, err)
				return err
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, 0, err
	}

	log.Debugf("commit %s success: total=%d", tx, len(tx.txs))

	return resultx.New(), 0, nil
}

func (tx *compositeTx) Rollback(ctx context.Context) (proto.Result, uint16, error) {
	ctx, span := Tracer.Start(ctx, "compositeTx.Rollback")
	defer span.End()
	if !tx.closed.CAS(false, true) {
		return nil, 0, errTxClosed
	}

	defer func() { // cleanup
		tx.rt = nil
		tx.txs = nil
	}()

	var g errgroup.Group
	for k, v := range tx.txs {
		k, v := k, v
		g.Go(func() error {
			_, _, err := v.Rollback(ctx)
			if err != nil {
				log.Errorf("rollback %s for group %s failed: %v", tx, k, err)
				return err
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, 0, err
	}

	log.Debugf("rollback %s success: total=%d", tx, len(tx.txs))

	return resultx.New(), 0, nil
}

type atomTx struct {
	closed atomic.Bool
	parent *AtomDB
	bc     *mysql.BackendConnection
}

func (tx *atomTx) Commit(ctx context.Context) (res proto.Result, warn uint16, err error) {
	_ = ctx
	if !tx.closed.CAS(false, true) {
		err = errTxClosed
		return
	}
	defer tx.dispose()
	if res, err = tx.bc.ExecuteWithWarningCount("commit", true); err != nil {
		return
	}

	var affected, lastInsertId uint64

	if affected, err = res.RowsAffected(); err != nil {
		return
	}
	if lastInsertId, err = res.LastInsertId(); err != nil {
		return
	}

	res = resultx.New(resultx.WithRowsAffected(affected), resultx.WithLastInsertID(lastInsertId))
	return
}

func (tx *atomTx) Rollback(ctx context.Context) (res proto.Result, warn uint16, err error) {
	if !tx.closed.CAS(false, true) {
		err = errTxClosed
		return
	}
	defer tx.dispose()
	res, err = tx.bc.ExecuteWithWarningCount("rollback", true)
	return
}

func (tx *atomTx) Call(ctx context.Context, sql string, args ...proto.Value) (res proto.Result, warn uint16, err error) {
	if len(args) > 0 {
		res, err = tx.bc.PrepareQueryArgs(sql, args)
	} else {
		res, err = tx.bc.ExecuteWithWarningCountIterRow(sql)
	}
	return
}

func (tx *atomTx) CallFieldList(ctx context.Context, table, wildcard string) ([]proto.Field, error) {
	// TODO: choose table
	var err error
	if err = tx.bc.WriteComFieldList(table, wildcard); err != nil {
		return nil, perrors.WithStack(err)
	}
	return tx.bc.ReadColumnDefinitions()
}

func (tx *atomTx) dispose() {
	defer func() {
		tx.parent = nil
		tx.bc = nil
	}()

	cnt := tx.parent.pendingRequests.Dec()
	tx.parent.returnConnection(tx.bc)
	if cnt == 0 && tx.parent.closed.Load() {
		tx.parent.pool.Close()
	}
}

type AtomDB struct {
	mu sync.Mutex

	id string

	weight proto.Weight
	pool   *pools.ResourcePool

	closed atomic.Bool

	pendingRequests atomic.Int64
}

func NewAtomDB(node *config.Node) *AtomDB {
	if node == nil {
		return nil
	}
	r, w, err := node.GetReadAndWriteWeight()
	if err != nil {
		return nil
	}
	db := &AtomDB{
		id:     node.Name,
		weight: proto.Weight{R: int32(r), W: int32(w)},
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", node.Username, node.Password, node.Host, node.Port, node.Database, node.Parameters.String())
	connector, err := mysql.NewConnector(dsn)
	if err != nil {
		panic(err)
	}

	var (
		capacity    = config.GetConnPropCapacity(node.ConnProps, 8)
		maxCapacity = config.GetConnPropMaxCapacity(node.ConnProps, 64)
		idleTime    = config.GetConnPropIdleTime(node.ConnProps, 30*time.Minute)
	)

	db.pool = pools.NewResourcePool(func(ctx context.Context) (pools.Resource, error) {
		return connector.NewBackendConnection(ctx)
	}, capacity, maxCapacity, idleTime, 1, nil)

	return db
}

func (db *AtomDB) Variable(ctx context.Context, name string) (interface{}, error) {
	if db.closed.Load() {
		return nil, perrors.Errorf("the db instance '%s' is closed already", db.id)
	}

	// 1. search from transient variables
	vars := rcontext.TransientVariables(ctx)
	if v, ok := vars[name]; ok {
		return v, nil
	}

	// 2. search from remote variables
	c, err := db.borrowConnection(ctx)
	if err != nil {
		return nil, perrors.WithStack(err)
	}

	defer db.returnConnection(c)
	defer db.pending()()

	if vars, err = c.PersistVariables(); err != nil {
		return nil, perrors.WithStack(err)
	}

	if v, ok := vars[name]; ok {
		return v, nil
	}

	return nil, nil
}

func (db *AtomDB) begin(ctx context.Context) (*atomTx, error) {
	if db.closed.Load() {
		return nil, perrors.Errorf("the db instance '%s' is closed already", db.id)
	}

	var (
		bc  *mysql.BackendConnection
		err error
	)

	if bc, err = db.borrowConnection(ctx); err != nil {
		return nil, perrors.WithStack(err)
	}

	db.pendingRequests.Inc()

	dispose := func() {
		// cleanup if failed to begin tx
		cnt := db.pendingRequests.Dec()
		db.returnConnection(bc)
		if cnt == 0 && db.closed.Load() {
			db.pool.Close()
		}
	}

	var res proto.Result
	if res, err = bc.ExecuteWithWarningCount("begin", true); err != nil {
		defer dispose()
		return nil, perrors.WithStack(err)
	}

	// NOTICE: must consume the result
	if _, err = res.RowsAffected(); err != nil {
		defer dispose()
		return nil, perrors.WithStack(err)
	}

	return &atomTx{parent: db, bc: bc}, nil
}

func (db *AtomDB) CallFieldList(ctx context.Context, table, wildcard string) ([]proto.Field, error) {
	if db.closed.Load() {
		return nil, perrors.Errorf("the db instance '%s' is closed already", db.id)
	}

	var (
		bc  *mysql.BackendConnection
		err error
	)

	if bc, err = db.borrowConnection(ctx); err != nil {
		return nil, perrors.WithStack(err)
	}

	defer db.returnConnection(bc)
	defer db.pending()()

	if err = bc.WriteComFieldList(table, wildcard); err != nil {
		return nil, perrors.WithStack(err)
	}

	return bc.ReadColumnDefinitions()
}

func (db *AtomDB) Call(ctx context.Context, sql string, args ...proto.Value) (res proto.Result, warn uint16, err error) {
	if db.closed.Load() {
		err = perrors.Errorf("the db instance '%s' is closed already", db.id)
		return
	}

	var bc *mysql.BackendConnection

	if bc, err = db.borrowConnection(ctx); err != nil {
		err = perrors.WithStack(err)
		return
	}

	undoPending := db.pending()

	if err = bc.SyncVariables(rcontext.TransientVariables(ctx)); err != nil {
		undoPending()
		db.returnConnection(bc)
		return
	}

	if len(args) > 0 {
		res, err = bc.PrepareQueryArgs(sql, args)
	} else {
		res, err = bc.ExecuteWithWarningCountIterRow(sql)
	}

	if err != nil {
		undoPending()
		db.returnConnection(bc)
		return
	}

	res.(*mysql.RawResult).SetCloser(func() error {
		undoPending()
		db.returnConnection(bc)
		return nil
	})

	return
}

func (db *AtomDB) Close() error {
	if db.closed.CAS(false, true) {
		if db.pendingRequests.Load() == 0 {
			db.pool.Close()
		}
	}
	return nil
}

func (db *AtomDB) pending() func() {
	db.pendingRequests.Inc()
	return func() {
		// close pool if atom db is marked as closed, and no requests.
		if cnt := db.pendingRequests.Dec(); cnt == 0 && db.closed.Load() {
			db.pool.Close()
		}
	}
}

func (db *AtomDB) ID() string {
	return db.id
}

func (db *AtomDB) IdleTimeout() time.Duration {
	return db.pool.IdleTimeout()
}

func (db *AtomDB) MaxCapacity() int {
	return int(db.pool.MaxCap())
}

func (db *AtomDB) Capacity() int {
	return int(db.pool.Capacity())
}

func (db *AtomDB) Weight() proto.Weight {
	return db.weight
}

func (db *AtomDB) SetCapacity(capacity int) error {
	return db.pool.SetCapacity(capacity)
}

func (db *AtomDB) SetMaxCapacity(maxCapacity int) error {
	// TODO: how to set max capacity?
	return nil
}

func (db *AtomDB) SetIdleTimeout(idleTimeout time.Duration) error {
	db.pool.SetIdleTimeout(idleTimeout)
	return nil
}

func (db *AtomDB) SetWeight(weight proto.Weight) error {
	db.weight = weight
	return nil
}

func (db *AtomDB) borrowConnection(ctx context.Context) (*mysql.BackendConnection, error) {
	bcp := (*BackendResourcePool)(db.pool)
	//var (
	//	active0, available0 = db.pool.Active(), db.pool.Available()
	//)
	res, err := bcp.Get(ctx)
	// log.Infof("^^^^^ borrow conn: %d/%d => %d/%d", available0, active0, db.pool.Active(), db.pool.Available())
	if err != nil {
		return nil, perrors.WithStack(err)
	}
	return res, nil
}

func (db *AtomDB) returnConnection(bc *mysql.BackendConnection) {
	db.pool.Put(bc)
	// log.Infof("^^^^^ return conn: active=%d, available=%d", db.pool.Active(), db.pool.Available())
}

type defaultRuntime namespace.Namespace

func (pi *defaultRuntime) Version(ctx context.Context) (string, error) {
	version, err := pi.Namespace().DB0(ctx).Variable(ctx, "@@version")
	if err != nil {
		return "", perrors.WithStack(err)
	}

	if ret, ok := version.(string); ok {
		return ret, nil
	}

	return "", perrors.New("no version found")
}

func (pi *defaultRuntime) Begin(ctx context.Context) (proto.Tx, error) {
	_, span := Tracer.Start(ctx, "defaultRuntime.Begin")
	defer span.End()
	tx := &compositeTx{
		id:  nextTxID(),
		rt:  pi,
		txs: make(map[string]*atomTx),
	}
	log.Debugf("begin transaction: %s", tx)
	return tx, nil
}

func (pi *defaultRuntime) Namespace() *namespace.Namespace {
	return (*namespace.Namespace)(pi)
}

func (pi *defaultRuntime) Query(ctx context.Context, db string, query string, args ...proto.Value) (proto.Result, error) {
	ctx = rcontext.WithRead(ctx)
	return pi.call(ctx, db, query, args...)
}

func (pi *defaultRuntime) Exec(ctx context.Context, db string, query string, args ...proto.Value) (proto.Result, error) {
	ctx = rcontext.WithWrite(ctx)
	res, err := pi.call(ctx, db, query, args...)
	if err != nil {
		return nil, perrors.WithStack(err)
	}

	if closer, ok := res.(io.Closer); ok {
		defer func() {
			_ = closer.Close()
		}()
	}
	return res, nil
}

func (pi *defaultRuntime) Execute(ctx *proto.Context) (res proto.Result, warn uint16, err error) {
	var span trace.Span
	ctx.Context, span = Tracer.Start(ctx.Context, "defaultRuntime.Execute")
	span.SetAttributes(attribute.Key("sql").String(ctx.GetQuery()))
	execStart := time.Now()
	defer func() {
		span.End()
		since := time.Since(execStart)
		metrics.ExecuteDuration.Observe(since.Seconds())
		if pi.Namespace().SlowThreshold() != 0 && since > pi.Namespace().SlowThreshold() {
			pi.Namespace().SlowLogger().Warnf("slow logs elapsed %v sql %s", since, ctx.GetQuery())
		}
	}()
	args := ctx.GetArgs()

	if rcontext.IsDirect(ctx.Context) {
		return pi.callDirect(ctx, args)
	}

	var (
		ru   = pi.Namespace().Rule()
		plan proto.Plan
	)

	ctx.Context = rcontext.WithHints(ctx.Context, ctx.Stmt.Hints)

	start := time.Now()

	var opt proto.Optimizer
	if opt, err = optimize.NewOptimizer(ru, ctx.Stmt.Hints, ctx.Stmt.StmtNode, args); err != nil {
		err = perrors.WithStack(err)
		return
	}

	if plan, err = opt.Optimize(ctx); err != nil {
		err = perrors.WithStack(err)
		return
	}
	metrics.OptimizeDuration.Observe(time.Since(start).Seconds())

	if res, err = plan.ExecIn(ctx, pi); err != nil {
		// TODO: how to warp error packet
		if sqlErr, ok := perrors.Cause(err).(*errors2.SQLError); ok {
			err = sqlErr
		} else {
			err = perrors.Wrapf(err, "failed to execute %T", plan)
		}
		return
	}

	return
}

func (pi *defaultRuntime) callDirect(ctx *proto.Context, args []proto.Value) (res proto.Result, warn uint16, err error) {
	res, warn, err = pi.Namespace().DB0(ctx.Context).Call(rcontext.WithWrite(ctx.Context), ctx.GetQuery(), args...)
	if err != nil {
		err = perrors.WithStack(err)
		return
	}
	return
}

func (pi *defaultRuntime) call(ctx context.Context, group, query string, args ...proto.Value) (proto.Result, error) {
	db := selectDB(ctx, group, pi.Namespace())
	if db == nil {
		return nil, perrors.Errorf("cannot get upstream database %s", group)
	}
	log.Debugf("call upstream: db=%s, id=%s, sql=\"%s\", args=%v", group, db.ID(), query, args)
	// TODO: how to pass warn???
	res, _, err := db.Call(ctx, query, args...)

	return res, err
}

// select db by group
func selectDB(ctx context.Context, group string, ns *namespace.Namespace) proto.DB {
	if len(group) < 1 { // empty db, select first
		if groups := ns.DBGroups(); len(groups) > 0 {
			group = groups[0]
		}
	}

	var (
		db       proto.DB
		hintType hint.Type
	)
	// write request
	if !rcontext.IsRead(ctx) {
		return ns.DBMaster(ctx, group)
	}
	// extracts hints
	hints := rcontext.Hints(ctx)
	for _, v := range hints {
		if v.Type == hint.TypeMaster || v.Type == hint.TypeSlave {
			hintType = v.Type
			break
		}
	}
	switch hintType {
	case hint.TypeMaster:
		db = ns.DBMaster(ctx, group)
	case hint.TypeSlave:
		db = ns.DBSlave(ctx, group)
	default:
		db = ns.DB(ctx, group)
	}
	return db
}

var (
	_txIds     *snowflake.Node
	_txIdsOnce sync.Once
)

func nextTxID() int64 {
	_txIdsOnce.Do(func() {
		_txIds, _ = snowflake.NewNode(rand2.Int63n(1024))
	})
	return _txIds.Generate().Int64()
}
