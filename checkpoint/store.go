package checkpoint

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	etlcp "github.com/kordar/goetl/checkpoint"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Store struct {
	DB                 *gorm.DB
	TableName          string
	Namespace          string
	DisableAutoMigrate bool
	UseCache           bool

	once    sync.Once
	cacheMu sync.RWMutex
	cache   map[string]etlcp.Cursor
}

func (s *Store) Save(ctx context.Context, key string, cursor etlcp.Cursor) error {
	if s.DB == nil {
		return errors.New("checkpoint store requires DB")
	}
	if key == "" {
		return errors.New("checkpoint store requires key")
	}
	if err := s.ensure(ctx); err != nil {
		return err
	}

	payload, err := json.Marshal(cursor)
	if err != nil {
		return err
	}

	row := checkpointRow{
		Namespace:     s.Namespace,
		CheckpointKey: key,
		CursorJSON:    string(payload),
		UpdatedAt:     time.Now().UTC(),
	}

	db := s.DB.WithContext(ctx).Table(s.tableName())
	if err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "namespace"},
			{Name: "checkpoint_key"},
		},
		DoUpdates: clause.AssignmentColumns([]string{"cursor_json", "updated_at"}),
	}).Create(&row).Error; err != nil {
		return err
	}
	return nil
}

func (s *Store) Load(ctx context.Context, key string) (etlcp.Cursor, error) {
	if s.UseCache {
		s.cacheMu.RLock()
		if s.cache != nil {
			if cur, ok := s.cache[key]; ok {
				s.cacheMu.RUnlock()
				return cur, nil
			}
		}
		s.cacheMu.RUnlock()
	}

	if s.DB == nil {
		return etlcp.Cursor{}, errors.New("checkpoint store requires DB")
	}
	if key == "" {
		return etlcp.Cursor{}, errors.New("checkpoint store requires key")
	}
	if err := s.ensure(ctx); err != nil {
		return etlcp.Cursor{}, err
	}

	var row checkpointRow
	db := s.DB.WithContext(ctx).Table(s.tableName())
	err := db.Where("namespace = ? AND checkpoint_key = ?", s.Namespace, key).Take(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) || errors.Is(err, sql.ErrNoRows) {
			return etlcp.Cursor{}, etlcp.ErrNotFound
		}
		return etlcp.Cursor{}, err
	}

	var cur etlcp.Cursor
	if row.CursorJSON != "" {
		dec := json.NewDecoder(strings.NewReader(row.CursorJSON))
		dec.UseNumber()
		if err := dec.Decode(&cur); err != nil {
			return etlcp.Cursor{}, err
		}
	}
	if s.UseCache {
		s.cacheMu.Lock()
		if s.cache == nil {
			s.cache = map[string]etlcp.Cursor{}
		}
		s.cache[key] = cur
		s.cacheMu.Unlock()
	}
	return cur, nil
}

func (s *Store) ensure(ctx context.Context) error {
	if s.DisableAutoMigrate {
		return nil
	}

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
	Namespace     string    `gorm:"primaryKey;size:128;column:namespace"`
	CheckpointKey string    `gorm:"primaryKey;size:256;column:checkpoint_key"`
	CursorJSON    string    `gorm:"type:longtext;column:cursor_json"`
	UpdatedAt     time.Time `gorm:"column:updated_at"`
}
