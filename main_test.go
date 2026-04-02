package goetlgorm_test

import (
	"context"
	"fmt"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/kordar/goetl"
	gormsource "github.com/kordar/goetl-gorm/source"
	"github.com/kordar/goetl/checkpoint"
	sinkdispatcher "github.com/kordar/goetl/dispatcher/sink"
	"github.com/kordar/goetl/engine"
)

type memSink struct {
	mu         sync.Mutex
	batches    [][]goetl.Message
	checkpoint checkpoint.CheckpointStore
}

func (s *memSink) Name() string { return "mem" }

func (s *memSink) WriteBatch(ctx context.Context, messages []goetl.Message) error {
	fmt.Println("write batch", messages)
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]goetl.Message, len(messages))
	copy(cp, messages)
	s.batches = append(s.batches, cp)
	fmt.Println("write batch", cp)
	if s.checkpoint != nil {
		_ = s.checkpoint.Save(ctx, s.Name(), checkpoint.Cursor{Values: []any{len(s.batches)}})
	}
	return nil
}

func TestEngine_WithSource_BasicFlow(t *testing.T) {

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
