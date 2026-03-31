package gormsource

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/kordar/goetl"
	"github.com/kordar/goetl/checkpoint"
)

func TestDBWalker_SQL_Paging(t *testing.T) {
	t.Parallel()
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })

	query := "SELECT id, name FROM t WHERE id > ? ORDER BY id LIMIT ?"
	queryRe := regexp.QuoteMeta(query)

	mock.ExpectQuery(queryRe).WithArgs(int64(0), 2).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(int64(1), "a").AddRow(int64(2), "b"))
	mock.ExpectQuery(queryRe).WithArgs(int64(2), 2).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(int64(3), "c"))
	mock.ExpectQuery(queryRe).WithArgs(int64(3), 2).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}))

	w := &DBWalker{
		SQL:           sqlDB,
		CheckpointKey: "k",
		PageSize:      2,
		BuildQuery: func(ctx context.Context, cur checkpoint.Cursor, limit int) (string, []any, error) {
			_ = ctx
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
		MapRow: func(row map[string]any) (any, error) {
			return row, nil
		},
		BuildAck: func(row map[string]any, cur checkpoint.Cursor) (string, error) {
			_ = cur
			return fmt.Sprintf("%d", row["id"].(int64)), nil
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	out := make(chan goetl.Message, 16)
	done := make(chan error, 1)
	go func() {
		done <- w.Start(ctx, out)
		close(out)
	}()

	var got []int64
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timeout: %v", ctx.Err())
		case msg, ok := <-out:
			if !ok {
				err := <-done
				if err != nil {
					t.Fatalf("start: %v", err)
				}
				if len(got) != 3 {
					t.Fatalf("count=%d want=3", len(got))
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatalf("sqlmock expectations: %v", err)
				}
				return
			}
			if msg.Checkpoint != "k" {
				t.Fatalf("checkpoint=%s want=k", msg.Checkpoint)
			}
			if msg.Ack == "" {
				t.Fatalf("ack is empty")
			}
			if len(msg.Cursor.Values) != 1 {
				t.Fatalf("cursor=%v", msg.Cursor)
			}
			id := msg.Cursor.Values[0].(int64)
			got = append(got, id)
		}
	}
}

func TestDBWalker_SQL_MaxItems(t *testing.T) {
	t.Parallel()
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })

	query := "SELECT id FROM t WHERE id > ? ORDER BY id LIMIT ?"
	queryRe := regexp.QuoteMeta(query)
	mock.ExpectQuery(queryRe).WithArgs(int64(0), 2).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(1)).AddRow(int64(2)))

	w := &DBWalker{
		SQL:           sqlDB,
		CheckpointKey: "k",
		PageSize:      2,
		MaxItems:      1,
		BuildQuery: func(ctx context.Context, cur checkpoint.Cursor, limit int) (string, []any, error) {
			_ = ctx
			last := int64(0)
			if len(cur.Values) > 0 {
				last = cur.Values[0].(int64)
			}
			return query, []any{last, limit}, nil
		},
		ExtractCursor: func(row map[string]any) (checkpoint.Cursor, error) {
			id := row["id"].(int64)
			return checkpoint.Cursor{Values: []any{id}}, nil
		},
		BuildAck: func(row map[string]any, cur checkpoint.Cursor) (string, error) {
			_ = cur
			return fmt.Sprintf("%d", row["id"].(int64)), nil
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out := make(chan goetl.Message, 16)
	if err := w.Start(ctx, out); err != nil {
		t.Fatalf("start: %v", err)
	}
	select {
	case msg := <-out:
		if msg.Ack != "1" {
			t.Fatalf("ack=%s want=1", msg.Ack)
		}
	default:
		t.Fatalf("no message")
	}
	if len(out) != 0 {
		t.Fatalf("unexpected extra messages: %d", len(out))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestDBWalkerTicker_Periodic(t *testing.T) {
	t.Parallel()
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })

	query := "SELECT id FROM t WHERE id > ? ORDER BY id LIMIT ?"
	queryRe := regexp.QuoteMeta(query)
	mock.ExpectQuery(queryRe).WithArgs(int64(0), 1).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))
	mock.ExpectQuery(queryRe).WithArgs(int64(0), 1).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))

	w := &DBWalker{
		SQL:           sqlDB,
		CheckpointKey: "k",
		PageSize:      1,
		BuildQuery: func(ctx context.Context, cur checkpoint.Cursor, limit int) (string, []any, error) {
			_ = ctx
			_ = cur
			return query, []any{int64(0), limit}, nil
		},
		ExtractCursor: func(row map[string]any) (checkpoint.Cursor, error) {
			_ = row
			return checkpoint.Cursor{}, nil
		},
	}

	tk := NewDBWalkerTicker(w, 50*time.Millisecond, 50*time.Millisecond, true)
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan goetl.Message, 1)
	done := make(chan error, 1)
	go func() { done <- tk.Start(ctx, out) }()

	time.Sleep(70 * time.Millisecond)
	cancel()
	_ = <-done

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}
