package checkpoint

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	etlcp "github.com/kordar/goetl/checkpoint"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

func TestStore_Load_NotFound(t *testing.T) {
	t.Parallel()

	db, mock, cleanup := newMockDB(t)
	defer cleanup()

	mock.ExpectQuery("SELECT (.+)etl_checkpoints(.+)").
		WithArgs("ns", "k1", sqlmock.AnyArg()).
		WillReturnError(sql.ErrNoRows)

	s := &Store{DB: db, Namespace: "ns", DisableAutoMigrate: true}
	_, err := s.Load(context.Background(), "k1")
	if !reflect.DeepEqual(err, etlcp.ErrNotFound) {
		t.Fatalf("err=%v want=%v", err, etlcp.ErrNotFound)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestStore_SaveThenLoad(t *testing.T) {
	t.Parallel()

	db, mock, cleanup := newMockDB(t)
	defer cleanup()

	mock.ExpectExec("(?s)INSERT (.+)etl_checkpoints(.+)").
		WithArgs("ns", "k1", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	curJSON := `{"values":[1,"x"],"meta":{"checkpoint":"k1","p":"p1"}}`
	mock.ExpectQuery("SELECT (.+)etl_checkpoints(.+)").
		WithArgs("ns", "k1", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"namespace", "checkpoint_key", "cursor_json", "updated_at"}).
			AddRow("ns", "k1", curJSON, time.Now()))

	s := &Store{DB: db, Namespace: "ns", DisableAutoMigrate: true}

	want := etlcp.Cursor{Values: []any{json.Number("1"), "x"}, Meta: map[string]any{"checkpoint": "k1", "p": "p1"}}
	if err := s.Save(context.Background(), "k1", want); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := s.Load(context.Background(), "k1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("cursor=%v want=%v", got, want)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func newMockDB(t *testing.T) (*gorm.DB, sqlmock.Sqlmock, func()) {
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
