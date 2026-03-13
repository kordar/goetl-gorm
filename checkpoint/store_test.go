package checkpointdb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/kordar/go-etl/checkpoint"
	"gorm.io/gorm"
)

func TestStore_SaveLoad(t *testing.T) {
	t.Parallel()

	db, mock, cleanup := openTestDB(t)
	defer cleanup()
	s := &Store{DB: db, Namespace: "job_a"}
	s.once.Do(func() {})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mock.ExpectQuery("SELECT (.+) FROM `etl_checkpoints` WHERE namespace = \\? AND key = \\?(.+)LIMIT \\?").
		WithArgs("job_a", "k1", 1).
		WillReturnRows(sqlmock.NewRows([]string{"namespace", "key", "value", "updated_at"}))

	_, err := s.Load(ctx, "k1")
	if err == nil || !errors.Is(err, checkpoint.ErrNotFound) {
		t.Fatalf("load err=%v want ErrNotFound", err)
	}

	mock.ExpectExec("INSERT INTO `etl_checkpoints`(.+)ON DUPLICATE KEY UPDATE(.+)").
		WithArgs("job_a", "k1", "v1", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := s.Save(ctx, "k1", "v1"); err != nil {
		t.Fatalf("save: %v", err)
	}

	mock.ExpectQuery("SELECT (.+) FROM `etl_checkpoints` WHERE namespace = \\? AND key = \\?(.+)LIMIT \\?").
		WithArgs("job_a", "k1", 1).
		WillReturnRows(sqlmock.NewRows([]string{"namespace", "key", "value", "updated_at"}).AddRow("job_a", "k1", "v1", time.Now()))

	v, err := s.Load(ctx, "k1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if v != "v1" {
		t.Fatalf("value=%s want=%s", v, "v1")
	}

	mock.ExpectExec("INSERT INTO `etl_checkpoints`(.+)ON DUPLICATE KEY UPDATE(.+)").
		WithArgs("job_a", "k1", "v2", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := s.Save(ctx, "k1", "v2"); err != nil {
		t.Fatalf("save2: %v", err)
	}

	mock.ExpectQuery("SELECT (.+) FROM `etl_checkpoints` WHERE namespace = \\? AND key = \\?(.+)LIMIT \\?").
		WithArgs("job_a", "k1", 1).
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
		mock.ExpectExec("INSERT INTO `etl_checkpoints`(.+)ON DUPLICATE KEY UPDATE(.+)").
			WithArgs("job_b", "k", sqlmock.AnyArg(), sqlmock.AnyArg()).
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

	mock.ExpectQuery("SELECT (.+) FROM `etl_checkpoints` WHERE namespace = \\? AND key = \\?(.+)LIMIT \\?").
		WithArgs("job_b", "k", 1).
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

	var db *gorm.DB

	// db, err := gorm.Open(mysql.New(mysql.Config{
	// 	Conn:                      sqlDB,
	// 	SkipInitializeWithVersion: true,
	// }), &gorm.Config{SkipDefaultTransaction: true})
	// if err != nil {
	// 	_ = sqlDB.Close()
	// 	t.Fatalf("gorm open: %v", err)
	// }

	cleanup := func() { _ = sqlDB.Close() }
	return db, mock, cleanup
}
