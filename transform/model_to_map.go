package gormtransform

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/kordar/go-etl"
)

type ModelToMap struct {
	Field string
}

func (t *ModelToMap) Name() string { return "gorm_model_to_map" }

func (t *ModelToMap) Transform(ctx context.Context, r *etl.Record) (*etl.Record, error) {
	_ = ctx
	if r == nil {
		return nil, nil
	}
	if r.Data == nil {
		return r, nil
	}
	field := t.Field
	if field == "" {
		field = "model"
	}
	v, ok := r.Data[field]
	if !ok || v == nil {
		return r, nil
	}
	m, err := toMap(v)
	if err != nil {
		return nil, err
	}
	r.Data = m
	return r, nil
}

func toMap(v any) (map[string]any, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	if m == nil {
		return nil, errors.New("model to map produced nil")
	}
	return m, nil
}
