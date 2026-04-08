package ack

import (
	"sync"

	"github.com/kordar/goetl/checkpoint"
)

type CursorMaxFunc func(old, new *checkpoint.Cursor) *checkpoint.Cursor

type Tracker struct {
	mu sync.Mutex

	data map[string]*checkpoint.Cursor

	defaultMaxFn   CursorMaxFunc
	partitionMaxFn map[string]CursorMaxFunc

	closed bool
}

func NewTracker(defaultFn CursorMaxFunc) *Tracker {
	return &Tracker{
		data:           make(map[string]*checkpoint.Cursor),
		defaultMaxFn:   defaultFn,
		partitionMaxFn: make(map[string]CursorMaxFunc),
	}
}

// 设置某个 partition 专属 maxFn
func (t *Tracker) SetPartitionMaxFn(partition string, fn CursorMaxFunc) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.partitionMaxFn[partition] = fn
}

// 获取 maxFn（带优先级）
func (t *Tracker) getMaxFn(partition string) CursorMaxFunc {
	if fn, ok := t.partitionMaxFn[partition]; ok {
		return fn
	}
	return t.defaultMaxFn
}

func (t *Tracker) Add(partition string, cursor *checkpoint.Cursor) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return
	}

	old, ok := t.data[partition]
	if !ok {
		t.data[partition] = cursor
		return
	}

	maxFn := t.getMaxFn(partition)

	if maxFn != nil {
		t.data[partition] = maxFn(old, cursor)
		return
	}

	// fallback：直接覆盖
	t.data[partition] = cursor
}

func (t *Tracker) Commit() map[string]*checkpoint.Cursor {
	t.mu.Lock()
	defer t.mu.Unlock()

	result := make(map[string]*checkpoint.Cursor, len(t.data))
	for k, v := range t.data {
		result[k] = v
	}

	return result
}

func (t *Tracker) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.closed = true
	t.data = nil
	t.partitionMaxFn = nil
	return nil
}
