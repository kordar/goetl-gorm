package checkpointdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/kordar/goetl/checkpoint"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

func TestStore_SaveLoad(t *testing.T) {
	t.Parallel()

	db, mock, cleanup := openTestDB(t)
	defer cleanup()
	s := &Store{DB: db, Namespace: "job_a"}
	s.once.Do(func() {})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mock.ExpectQuery("SELECT (.+)etl_checkpoints(.+)").
		WillReturnError(sql.ErrNoRows)

	_, err := s.Load(ctx, "k1")
	if err == nil || !errors.Is(err, checkpoint.ErrNotFound) {
		t.Fatalf("load err=%v want ErrNotFound", err)
	}

	mock.ExpectExec("(?s)INSERT (.+)etl_checkpoints(.+)").
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := s.Save(ctx, "k1", "v1"); err != nil {
		t.Fatalf("save: %v", err)
	}

	mock.ExpectQuery("SELECT (.+)etl_checkpoints(.+)").
		WillReturnRows(sqlmock.NewRows([]string{"namespace", "key", "value", "updated_at"}).AddRow("job_a", "k1", "v1", time.Now()))

	v, err := s.Load(ctx, "k1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if v != "v1" {
		t.Fatalf("value=%s want=%s", v, "v1")
	}

	mock.ExpectExec("(?s)INSERT (.+)etl_checkpoints(.+)").
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := s.Save(ctx, "k1", "v2"); err != nil {
		t.Fatalf("save2: %v", err)
	}

	mock.ExpectQuery("SELECT (.+)etl_checkpoints(.+)").
		WillReturnRows(sqlmock.NewRows([]string{"namespace", "key", "value", "updated_at"}).AddRow("job_a", "k1", "v2", time.Now()))

	v, err = s.Load(ctx, "k1")
	if err != nil {
		t.Fatalf("load2: %v", err)
	}
	if v != "v2" {
		t.Fatalf("value2=%s want=%s", v, "v2")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestStore_ConcurrentSave(t *testing.T) {
	t.Parallel()

	db, mock, cleanup := openTestDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	s := &Store{DB: db, Namespace: "job_b"}
	s.once.Do(func() {})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 50; i++ {
		mock.ExpectExec("(?s)INSERT (.+)etl_checkpoints(.+)").
			WillReturnResult(sqlmock.NewResult(1, 1))
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_ = s.Save(ctx, "k", fmt.Sprintf("v%d", n))
		}(i)
	}
	wg.Wait()

	mock.ExpectQuery("SELECT (.+)etl_checkpoints(.+)").
		WillReturnRows(sqlmock.NewRows([]string{"namespace", "key", "value", "updated_at"}).AddRow("job_b", "k", "v", time.Now()))

	_, err := s.Load(ctx, "k")
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func openTestDB(t *testing.T) (*gorm.DB, sqlmock.Sqlmock, func()) {
	t.Helper()

	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}

	db, err := gorm.Open(sqlmockDialector{conn: sqlDB}, &gorm.Config{SkipDefaultTransaction: true, DryRun: false})
	if err != nil {
		_ = sqlDB.Close()
		t.Fatalf("gorm open: %v", err)
	}
	db = db.Session(&gorm.Session{DryRun: false})
	db.ConnPool = sqlDB
	if db.Statement != nil {
		db.Statement.ConnPool = sqlDB
	}

	cleanup := func() { _ = sqlDB.Close() }
	return db, mock, cleanup
}

type sqlmockDialector struct {
	conn *sql.DB
}

func (d sqlmockDialector) Name() string { return "mysql" }

func (d sqlmockDialector) Initialize(db *gorm.DB) error {
	db.ConnPool = d.conn
	if db.Statement == nil {
		db.Statement = &gorm.Statement{DB: db, ConnPool: d.conn, Context: context.Background()}
	} else {
		db.Statement.ConnPool = d.conn
	}
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{
		CreateClauses: []string{"INSERT", "VALUES", "ON CONFLICT"},
		QueryClauses:  []string{"SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT"},
		UpdateClauses: []string{"UPDATE", "SET", "WHERE", "ORDER BY", "LIMIT"},
		DeleteClauses: []string{"DELETE", "FROM", "WHERE", "ORDER BY", "LIMIT"},
	})
	return nil
}

func (d sqlmockDialector) Migrator(db *gorm.DB) gorm.Migrator {
	_ = db
	return nil
}

func (d sqlmockDialector) DataTypeOf(field *schema.Field) string {
	_ = field
	return ""
}

func (d sqlmockDialector) DefaultValueOf(field *schema.Field) clause.Expression {
	_ = field
	return clause.Expr{}
}

func (d sqlmockDialector) BindVarTo(w clause.Writer, stmt *gorm.Statement, v any) {
	_ = stmt
	_ = v
	w.WriteByte('?')
}

func (d sqlmockDialector) QuoteTo(w clause.Writer, str string) {
	w.WriteByte('`')
	w.WriteString(str)
	w.WriteByte('`')
}

func (d sqlmockDialector) Explain(sql string, vars ...any) string {
	_ = vars
	return sql
}
