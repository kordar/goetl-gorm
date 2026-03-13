package checkpointdb

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/kordar/go-etl/checkpoint"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Store struct {
	DB        *gorm.DB
	TableName string
	Namespace string

	once sync.Once
}

func (s *Store) Save(ctx context.Context, key string, value string) error {
	if s.DB == nil {
		return errors.New("checkpoint store requires DB")
	}
	if err := s.ensure(ctx); err != nil {
		return err
	}

	row := checkpointRow{
		Namespace: s.Namespace,
		Key:       key,
		Value:     value,
		UpdatedAt: time.Now().UTC(),
	}

	db := s.DB.WithContext(ctx).Table(s.tableName())
	return db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "namespace"},
			{Name: "key"},
		},
		DoUpdates: clause.AssignmentColumns([]string{"value", "updated_at"}),
	}).Create(&row).Error
}

func (s *Store) Load(ctx context.Context, key string) (string, error) {
	if s.DB == nil {
		return "", errors.New("checkpoint store requires DB")
	}
	if err := s.ensure(ctx); err != nil {
		return "", err
	}

	var row checkpointRow
	db := s.DB.WithContext(ctx).Table(s.tableName())
	err := db.Where("namespace = ? AND key = ?", s.Namespace, key).Take(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", checkpoint.ErrNotFound
		}
		return "", err
	}
	return row.Value, nil
}

func (s *Store) ensure(ctx context.Context) error {
	var err error
	s.once.Do(func() {
		db := s.DB.WithContext(ctx).Table(s.tableName())
		err = db.AutoMigrate(&checkpointRow{})
	})
	return err
}

func (s *Store) tableName() string {
	if s.TableName != "" {
		return s.TableName
	}
	return "etl_checkpoints"
}

type checkpointRow struct {
	Namespace string `gorm:"primaryKey;size:128"`
	Key       string `gorm:"primaryKey;size:256"`
	Value     string `gorm:"type:text"`
	UpdatedAt time.Time
}
