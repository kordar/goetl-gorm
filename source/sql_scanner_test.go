package gormsource

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/kordar/go-etl"
	"github.com/kordar/go-etl/components/memory"
	"gorm.io/gorm"
)

func TestSQLScanner_Resume_NoLossNoDup(t *testing.T) {
	t.Parallel()

	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })

	var db *gorm.DB
	query := "SELECT id, name FROM sql_users WHERE id > ? ORDER BY id LIMIT 10"
	queryRe := regexp.QuoteMeta(query)

	mockBatch := func(cursor int64, startID int64, n int) *sqlmock.Rows {
		rows := sqlmock.NewRows([]string{"id", "name"})
		for i := 0; i < n; i++ {
			id := startID + int64(i)
			rows.AddRow(id, strconv.FormatInt(id, 10))
		}
		mock.ExpectQuery(queryRe).WithArgs(cursor).WillReturnRows(rows)
		return rows
	}

	mockEmpty := func(cursor int64) {
		rows := sqlmock.NewRows([]string{"id", "name"})
		mock.ExpectQuery(queryRe).WithArgs(cursor).WillReturnRows(rows)
	}

	store := memory.NewCheckpointStore()

	build := func(ctx context.Context, cursor int64) (string, []any, error) {
		_ = ctx
		return query, []any{cursor}, nil
	}

	extract := func(row map[string]any) (int64, error) {
		v, ok := row["id"]
		if !ok {
			return 0, fmt.Errorf("missing id")
		}
		switch x := v.(type) {
		case int64:
			return x, nil
		case int:
			return int64(x), nil
		case int32:
			return int64(x), nil
		case int16:
			return int64(x), nil
		case int8:
			return int64(x), nil
		case uint64:
			return int64(x), nil
		case uint:
			return int64(x), nil
		case uint32:
			return int64(x), nil
		case uint16:
			return int64(x), nil
		case uint8:
			return int64(x), nil
		case string:
			return strconv.ParseInt(x, 10, 64)
		default:
			return 0, fmt.Errorf("unsupported id type: %T", v)
		}
	}

	mockBatch(0, 1, 10)
	mockBatch(10, 11, 10)
	mockBatch(20, 21, 10)

	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()
	out1 := make(chan etl.Message, 128)
	sc1 := &SQLScanner[int64]{
		DB:            db,
		Store:         store,
		CheckpointKey: "sql_users",
		Codec:         Int64CursorCodec{},
		BuildQuery:    build,
		ExtractCursor: extract,
	}
	errCh1 := make(chan error, 1)
	go func() {
		errCh1 <- sc1.Start(ctx1, out1)
		close(out1)
	}()

	seen := map[int64]struct{}{}
	count1 := 0
	for msg := range out1 {
		count1++
		id, _ := strconv.ParseInt(msg.Checkpoint.Value, 10, 64)
		seen[id] = struct{}{}
		if err := store.Save(context.Background(), msg.Checkpoint.Key, msg.Checkpoint.Value); err != nil {
			t.Fatalf("save: %v", err)
		}
		if count1 == 30 {
			cancel1()
		}
	}
	_ = <-errCh1

	if count1 != 30 {
		t.Fatalf("count1=%d want=30", count1)
	}

	mockBatch(30, 31, 10)
	mockBatch(40, 41, 10)
	mockBatch(50, 51, 10)
	mockBatch(60, 61, 10)
	mockBatch(70, 71, 10)
	mockBatch(80, 81, 10)
	mockBatch(90, 91, 10)
	mockEmpty(100)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	out2 := make(chan etl.Message, 128)
	sc2 := &SQLScanner[int64]{
		DB:            db,
		Store:         store,
		CheckpointKey: "sql_users",
		Codec:         Int64CursorCodec{},
		BuildQuery:    build,
		ExtractCursor: extract,
	}
	errCh2 := make(chan error, 1)
	go func() {
		errCh2 <- sc2.Start(ctx2, out2)
		close(out2)
	}()

	count2 := 0
	for msg := range out2 {
		count2++
		id, _ := strconv.ParseInt(msg.Checkpoint.Value, 10, 64)
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate id across resume: %d", id)
		}
		if err := store.Save(context.Background(), msg.Checkpoint.Key, msg.Checkpoint.Value); err != nil {
			t.Fatalf("save: %v", err)
		}
	}
	if err := <-errCh2; err != nil {
		t.Fatalf("run2: %v", err)
	}

	if count2 != 70 {
		t.Fatalf("count2=%d want=70", count2)
	}

	v, err := store.Load(context.Background(), "sql_users")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if v != "100" {
		t.Fatalf("checkpoint=%s want=%s", v, "100")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}
