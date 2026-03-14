package gormsource

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/kordar/goetl"
	"github.com/kordar/goetl/checkpoint"
	"gorm.io/gorm"
)

type CursorCodec[T any] interface {
	Encode(v T) (string, error)
	Decode(s string) (T, error)
	Zero() T
}

type StringCursorCodec struct{}

func (StringCursorCodec) Encode(v string) (string, error) { return v, nil }
func (StringCursorCodec) Decode(s string) (string, error) { return s, nil }
func (StringCursorCodec) Zero() string                    { return "" }

type Int64CursorCodec struct{}

func (Int64CursorCodec) Encode(v int64) (string, error) { return fmt.Sprintf("%d", v), nil }
func (Int64CursorCodec) Decode(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	var v int64
	_, err := fmt.Sscanf(s, "%d", &v)
	return v, err
}
func (Int64CursorCodec) Zero() int64 { return 0 }

type TimeCursorCodec struct{}

func (TimeCursorCodec) Encode(v time.Time) (string, error) {
	return v.UTC().Format(time.RFC3339Nano), nil
}
func (TimeCursorCodec) Decode(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, s)
}
func (TimeCursorCodec) Zero() time.Time { return time.Time{} }

type QueryBuilder[T any] func(ctx context.Context, cursor T) (query string, args []any, err error)
type CursorExtractor[T any] func(row map[string]any) (T, error)
type RowMapper func(row map[string]any) (map[string]any, error)

type SQLScanner[T any] struct {
	DB            *gorm.DB
	Store         checkpoint.Store
	CheckpointKey string

	NameValue string
	Partition string

	Codec         CursorCodec[T]
	BuildQuery    QueryBuilder[T]
	ExtractCursor CursorExtractor[T]
	MapRow        RowMapper
}

func (s *SQLScanner[T]) Name() string {
	if s.NameValue != "" {
		return s.NameValue
	}
	return "gorm_sql_scanner"
}

func (s *SQLScanner[T]) Start(ctx context.Context, out chan<- goetl.Message) error {
	if s.DB == nil {
		return errors.New("sql scanner requires DB")
	}
	if s.CheckpointKey == "" {
		return errors.New("sql scanner requires CheckpointKey")
	}
	if s.Codec == nil {
		return errors.New("sql scanner requires Codec")
	}
	if s.BuildQuery == nil {
		return errors.New("sql scanner requires BuildQuery")
	}
	if s.ExtractCursor == nil {
		return errors.New("sql scanner requires ExtractCursor")
	}
	if s.MapRow == nil {
		s.MapRow = func(row map[string]any) (map[string]any, error) { return row, nil }
	}

	cur := s.Codec.Zero()
	if s.Store != nil {
		v, err := s.Store.Load(ctx, s.CheckpointKey)
		if err == nil && v != "" {
			decoded, derr := s.Codec.Decode(v)
			if derr != nil {
				return fmt.Errorf("invalid checkpoint value: key=%s: %w", s.CheckpointKey, derr)
			}
			cur = decoded
		}
		if err != nil && !errors.Is(err, checkpoint.ErrNotFound) {
			return err
		}
	}

	partition := s.Partition
	if partition == "" {
		partition = s.CheckpointKey
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		query, args, err := s.BuildQuery(ctx, cur)
		if err != nil {
			return err
		}

		rows, err := s.DB.WithContext(ctx).Raw(query, args...).Rows()
		if err != nil {
			return err
		}

		n, err := s.consumeRows(ctx, rows, partition, out, &cur)
		_ = rows.Close()
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
	}
}

func (s *SQLScanner[T]) consumeRows(ctx context.Context, rows *sql.Rows, partition string, out chan<- goetl.Message, cursor *T) (int, error) {
	cols, err := rows.Columns()
	if err != nil {
		return 0, err
	}

	count := 0
	for rows.Next() {
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		default:
		}

		raw := make([]any, len(cols))
		dest := make([]any, len(cols))
		for i := range raw {
			dest[i] = &raw[i]
		}

		if err := rows.Scan(dest...); err != nil {
			return count, err
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

		next, err := s.ExtractCursor(row)
		if err != nil {
			return count, err
		}
		cpValue, err := s.Codec.Encode(next)
		if err != nil {
			return count, err
		}

		data, err := s.MapRow(row)
		if err != nil {
			return count, err
		}

		msg := goetl.Message{
			Partition: partition,
			Record: &goetl.Record{
				ID:        cpValue,
				Timestamp: time.Now(),
				Source:    s.Name(),
				Data:      data,
			},
			Checkpoint: &goetl.Checkpoint{
				Key:   s.CheckpointKey,
				Value: cpValue,
			},
		}

		select {
		case <-ctx.Done():
			return count, ctx.Err()
		case out <- msg:
		}

		*cursor = next
		count++
	}

	if err := rows.Err(); err != nil {
		return count, err
	}
	return count, nil
}
