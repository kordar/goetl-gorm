package gormsource

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/kordar/goetl"
	"github.com/kordar/goetl/checkpoint"
	"gorm.io/gorm"
)

type DBQueryBuilder func(ctx context.Context, cursor checkpoint.Cursor, limit int) (query string, args []any, err error)
type DBCursorExtractor func(row map[string]any) (checkpoint.Cursor, error)
type DBRowMapper func(row map[string]any) (any, error)
type DBAckBuilder func(row map[string]any, cursor checkpoint.Cursor) (string, error)
type DBAttrsBuilder func(row map[string]any, cursor checkpoint.Cursor) (map[string]any, error)

type DBWalker struct {
	Gorm *gorm.DB
	SQL  *sql.DB

	Store         checkpoint.CheckpointStore
	CheckpointKey string

	NameValue string
	Partition string

	PageSize int
	MaxItems int

	BuildQuery    DBQueryBuilder
	ExtractCursor DBCursorExtractor
	MapRow        DBRowMapper
	BuildAck      DBAckBuilder
	BuildAttrs    DBAttrsBuilder
}

func (s *DBWalker) Name() string {
	if s.NameValue != "" {
		return s.NameValue
	}
	return "gorm_db_walker"
}

func (s *DBWalker) Start(ctx context.Context, out chan<- goetl.Message) error {
	if s.Gorm == nil && s.SQL == nil {
		return errors.New("db walker requires Gorm or SQL")
	}
	if s.CheckpointKey == "" {
		return errors.New("db walker requires CheckpointKey")
	}
	if s.BuildQuery == nil {
		return errors.New("db walker requires BuildQuery")
	}
	if s.ExtractCursor == nil {
		return errors.New("db walker requires ExtractCursor")
	}
	if s.MapRow == nil {
		s.MapRow = func(row map[string]any) (any, error) { return row, nil }
	}

	pageSize := s.PageSize
	if pageSize <= 0 {
		pageSize = 1000
	}

	cur := checkpoint.Cursor{}
	if s.Store != nil {
		v, err := s.Store.Load(ctx, s.CheckpointKey)
		fmt.Printf("load checkpoint: %v\n", v)
		if err == nil {
			cur = v
		}
		if err != nil && !errors.Is(err, checkpoint.ErrNotFound) {
			return err
		}
	}

	partition := s.Partition
	if partition == "" {
		partition = s.CheckpointKey
	}
	if partition == "" {
		partition = "default"
	}

	processed := 0
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if s.MaxItems > 0 && processed >= s.MaxItems {
			return nil
		}

		query, args, err := s.BuildQuery(ctx, cur, pageSize)
		if err != nil {
			return err
		}
		fmt.Printf("query: %s, args: %v\n", query, args)
		rows, err := s.queryRows(ctx, query, args...)
		if err != nil {
			return err
		}
		n, nextCur, err := s.consumeRows(ctx, rows, partition, out, processed)
		_ = rows.Close()
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
		processed += n
		cur = nextCur
	}
}

func (s *DBWalker) queryRows(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if s.SQL != nil {
		return s.SQL.QueryContext(ctx, query, args...)
	}
	return s.Gorm.WithContext(ctx).Raw(query, args...).Rows()
}

func (s *DBWalker) consumeRows(ctx context.Context, rows *sql.Rows, partition string, out chan<- goetl.Message, processed int) (int, checkpoint.Cursor, error) {
	cols, err := rows.Columns()
	if err != nil {
		return 0, checkpoint.Cursor{}, err
	}

	count := 0
	cur := checkpoint.Cursor{}
	for rows.Next() {
		if ctx.Err() != nil {
			return count, cur, ctx.Err()
		}
		if s.MaxItems > 0 && processed+count >= s.MaxItems {
			break
		}

		raw := make([]any, len(cols))
		dest := make([]any, len(cols))
		for i := range raw {
			dest[i] = &raw[i]
		}

		if err := rows.Scan(dest...); err != nil {
			return count, cur, err
		}

		row := make(map[string]any, len(cols))
		for i, c := range cols {
			v := raw[i]
			if b, ok := v.([]byte); ok {
				row[c] = string(b)
			} else {
				row[c] = v
			}
		}

		next, err := s.safeExtractCursor(row)
		if err != nil {
			return count, cur, err
		}
		data, err := s.safeMapRow(row)
		if err != nil {
			return count, cur, err
		}

		ackID := ""
		if s.BuildAck != nil {
			ackID, err = s.safeBuildAck(row, next)
			if err != nil {
				return count, cur, err
			}
		}
		attrs := map[string]any(nil)
		if s.BuildAttrs != nil {
			attrs, err = s.safeBuildAttrs(row, next)
			if err != nil {
				return count, cur, err
			}
		}

		rec := goetl.NewRecord(data).WithSource(s.Name()).WithTimestamp(time.Now().UnixMilli())
		msg := goetl.Message{
			Record:     rec,
			Partition:  partition,
			Ack:        ackID,
			Checkpoint: s.CheckpointKey,
			Cursor:     next,
			Attrs:      attrs,
		}

		select {
		case <-ctx.Done():
			return count, cur, ctx.Err()
		case out <- msg:
		}

		cur = next
		count++
	}

	if err := rows.Err(); err != nil {
		return count, cur, err
	}
	return count, cur, nil
}

func (s *DBWalker) safeExtractCursor(row map[string]any) (cur checkpoint.Cursor, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in ExtractCursor: %v\n%s", r, debug.Stack())
		}
	}()
	return s.ExtractCursor(row)
}

func (s *DBWalker) safeMapRow(row map[string]any) (data any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in MapRow: %v\n%s", r, debug.Stack())
		}
	}()
	return s.MapRow(row)
}

func (s *DBWalker) safeBuildAck(row map[string]any, cursor checkpoint.Cursor) (id string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in BuildAck: %v\n%s", r, debug.Stack())
		}
	}()
	return s.BuildAck(row, cursor)
}

func (s *DBWalker) safeBuildAttrs(row map[string]any, cursor checkpoint.Cursor) (attrs map[string]any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in BuildAttrs: %v\n%s", r, debug.Stack())
		}
	}()
	return s.BuildAttrs(row, cursor)
}
