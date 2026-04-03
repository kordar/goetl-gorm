package goetlgorm_test

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/kordar/goetl"
	checkpoint2 "github.com/kordar/goetl-gorm/checkpoint"
	gormsource "github.com/kordar/goetl-gorm/source"
	"github.com/kordar/goetl/checkpoint"
	sinkdispatcher "github.com/kordar/goetl/dispatcher/sink"
	"github.com/kordar/goetl/engine"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type memSink struct {
	mu         sync.Mutex
	batches    [][]goetl.Message
	checkpoint checkpoint.CheckpointStore
}

func TestEngine_WithSource_DBEnv(t *testing.T) {
	t.Parallel()

	dsn := os.Getenv("DB_DSN")

	if dsn == "" {
		t.Skip("DB_DSN is not set")
	}

	query := "select * from vd_despatch_order where id > ? order by id asc limit ?"

	gdb, err := gorm.Open(gormmysql.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open gorm mysql: %v", err)
	}
	if sqlDB, err := gdb.DB(); err == nil {
		t.Cleanup(func() { _ = sqlDB.Close() })
	}

	checkpointStore := &checkpoint2.Store{DB: gdb, UseCache: true}

	w := &gormsource.DBWalker{
		Gorm:          gdb,
		CheckpointKey: "test:db",
		Store:         checkpointStore,
		PageSize:      2,
		MaxItems:      50,
		BuildQuery: func(ctx context.Context, cur checkpoint.Cursor, limit int) (string, []any, error) {
			var last int64
			if len(cur.Values) > 0 {
				switch v := cur.Values[0].(type) {
				case int64:
					last = v
				case int:
					last = int64(v)
				}
			}
			return query, []any{last, limit}, nil
		},
		ExtractCursor: func(row map[string]any) (checkpoint.Cursor, error) {
			id := row["id"].(int64)
			return checkpoint.Cursor{Values: []any{id}}, nil
		},
		// ExtractCursor: func(row map[string]any) (checkpoint.Cursor, error) {
		// 	// 默认按 id 字段推进
		// 	id, ok := row["id"]
		// 	if !ok {
		// 		return checkpoint.Cursor{}, fmt.Errorf("no id field in row: %#v", row)
		// 	}
		// 	switch v := id.(type) {
		// 	case int64:
		// 		return checkpoint.Cursor{Values: []any{v}}, nil
		// 	case int:
		// 		return checkpoint.Cursor{Values: []any{int64(v)}}, nil
		// 	case string:
		// 		return checkpoint.Cursor{Values: []any{v}}, nil
		// 	default:
		// 		return checkpoint.Cursor{Values: []any{v}}, nil
		// 	}
		// },
	}

	ms := &memSink{}
	d := sinkdispatcher.NewBatchSinkDispatcher(ms).
		WithBatchSize(10).
		WithFlushInterval(1 * time.Second).
		WithQueueBuffer(100).
		WithBlocking(true)

	tk := gormsource.NewDBWalkerTicker(w, 2*time.Second, 2*time.Second, true)
	eng := engine.NewEngine().WithSource(tk).WithDispatcher(d)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	outCh, errCh := eng.Start(ctx)
	defer eng.Stop()

	consumed := 0
loop:
	for {
		select {
		case _, ok := <-outCh:
			if !ok {
				break loop
			}
			consumed++
		case err := <-errCh:
			if err != nil {
				t.Errorf("err: %v", err)
			}
		case <-ctx.Done():
			break loop
		}
	}
}

func (s *memSink) Name() string { return "mem" }

func (s *memSink) WriteBatch(ctx context.Context, messages []goetl.Message) error {
	fmt.Println("==========start write batch")
	for _, msg := range messages {
		fmt.Printf("msg=%#v\n, record=%+v\n", msg, msg.Record)
	}
	fmt.Println("==========end write batch")
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]goetl.Message, len(messages))
	copy(cp, messages)
	s.batches = append(s.batches, cp)
	if s.checkpoint != nil {
		_ = s.checkpoint.Save(ctx, s.Name(), checkpoint.Cursor{Values: []any{len(s.batches)}})
	}
	return nil
}

func TestEngine_WithSource_BasicFlow(t *testing.T) {
	// 生成checkpoint store
	t.Parallel()
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })

	query := "SELECT id, name FROM t WHERE id > ? ORDER BY id LIMIT ?"
	queryRe := regexp.QuoteMeta(query)

	for i := 0; i < 3; i++ {
		id := int64(1)
		mock.ExpectQuery(queryRe).WithArgs(int64(0), 1).
			WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(id, fmt.Sprintf("n-%d", id)))
	}
	mock.ExpectQuery(queryRe).WithArgs(int64(0), 1).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}))

	w := &gormsource.DBWalker{
		SQL:           sqlDB,
		CheckpointKey: "k",
		PageSize:      1,
		MaxItems:      1,
		BuildQuery: func(ctx context.Context, cur checkpoint.Cursor, limit int) (string, []any, error) {
			last := int64(0)
			if len(cur.Values) > 0 {
				if v, ok := cur.Values[0].(int64); ok {
					last = v
				}
			}
			return query, []any{last, limit}, nil
		},
		ExtractCursor: func(row map[string]any) (checkpoint.Cursor, error) {
			id := row["id"].(int64)
			return checkpoint.Cursor{Values: []any{id}}, nil
		},
	}

	ms := &memSink{}
	d := sinkdispatcher.NewBatchSinkDispatcher(ms).
		WithBatchSize(1).
		WithFlushInterval(10 * time.Millisecond).
		WithQueueBuffer(10)

	tk := gormsource.NewDBWalkerTicker(w, 1*time.Second, 1*time.Second, true)

	eng := engine.NewEngine().WithSource(tk).WithDispatcher(d)

	ctx := context.Background()
	eng.Run(ctx, func(m goetl.Message) {
		fmt.Println("msg", m)
	}, func(err error) {
		if err != nil {
			t.Errorf("run: %v", err)
		}
	})
	defer eng.Stop()

	time.Sleep(3500 * time.Millisecond)
}
