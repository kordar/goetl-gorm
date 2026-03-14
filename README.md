# goetl-gorm（基于 GORM 的 Source / Transform / 断点续扫）

`goetl-gorm` 是 [`goetl`](../goetl) 的一个扩展模块，用于在 **GORM** 上快速实现「数据库扫描 -> ETL Pipeline -> Sink」的数据采集任务，并支持 **checkpoint 断点续扫**。

## 能力

- **自定义 SQL 扫描（Source）**：完全自定义 SQL（query + args），支持分页/增量
- **断点续扫（恢复）**：启动时读取 `checkpoint.Store`，由游标 codec 解码后继续扫描
- **通用游标（cursor）**：支持 `int64 / string / time`，也支持你自定义任意结构（如 `(updated_at, id)`）
- **输出统一 Record**：每行输出为 `etl.Record`，默认 `Record.Data` 为列名到值的 `map[string]any`

## 模块结构

```
goetl-gorm
  source/     基于 GORM 的扫描器（Source）
  transform/  基于 GORM 的转换器（Transformer）
```

## 依赖

- 核心框架：`github.com/kordar/goetl`
- ORM：`gorm.io/gorm`

说明：

- 本模块单测基于 `sqlmock`，不依赖 sqlite 或外部数据库；业务侧按需引入 MySQL/Postgres 等 driver。

## Source：SQLScanner（自定义 SQL 通用扫描器）

实现见：[sql_scanner.go](source/sql_scanner.go)

你需要提供三件事即可运行：

- `BuildQuery(ctx, cursor)`：根据游标返回 `query string + args []any`
- `ExtractCursor(row)`：从一行数据中提取下一次游标（用于 checkpoint）
- `Codec`：游标编解码（checkpoint 的 `string <-> T`）

核心配置字段：

- `DB`：`*gorm.DB`
- `Store`：`checkpoint.Store`（可选；nil 表示不读取 checkpoint）
- `CheckpointKey`：checkpoint key（必填）
- `Codec`：游标 codec（内置 `Int64CursorCodec / StringCursorCodec / TimeCursorCodec`）
- `BuildQuery`：构建 SQL + 参数
- `ExtractCursor`：从 row 提取游标
- `MapRow`：可选，行映射（默认原样返回 `map[string]any`）

## Transform：ModelToMap（可选）

实现见：[model_to_map.go](transform/model_to_map.go)

用途：

- 当你希望 Source 先把原始 model 放到 `Record.Data["model"]`，再在 Pipeline 中统一展开为 `map[string]any` 时使用。

默认读取字段：

- `Record.Data["model"]`（可通过 `ModelToMap.Field` 修改）

## 使用示例（与 goetl Engine 集成）

下面示例展示如何使用 `SQLScanner` 做 ID 增量扫描并恢复：

```go
package main

import (
	"context"
	"log"
	"strconv"

	"github.com/kordar/goetl"
	"github.com/kordar/goetl/components/memory"
	"github.com/kordar/goetl/engine"
	"gorm.io/gorm"

	gormsource "github.com/kordar/goetl-gorm/source"
)

func main() {
	var db *gorm.DB
	if db == nil {
		log.Fatal("init gorm DB first")
	}

	cp := memory.NewCheckpointStore()

	build := func(ctx context.Context, cursor int64) (string, []any, error) {
		_ = ctx
		return "SELECT id, name FROM users WHERE id > ? ORDER BY id LIMIT 1000", []any{cursor}, nil
	}

	extract := func(row map[string]any) (int64, error) {
		v := row["id"]
		switch x := v.(type) {
		case int64:
			return x, nil
		case int:
			return int64(x), nil
		case string:
			return strconv.ParseInt(x, 10, 64)
		default:
			return 0, nil
		}
	}

	src := &gormsource.SQLScanner[int64]{
		DB:            db,
		Store:         cp,
		CheckpointKey: "users",
		Codec:         gormsource.Int64CursorCodec{},
		BuildQuery:    build,
		ExtractCursor: extract,
	}

	eng := &engine.Engine{
		Source:      src,
		Pipeline:    etl.NewPipeline(),
		Sink:        /* your sink */,
		Checkpoints: cp,
		Options: engine.Options{
			QueueBuffer:    1024,
			MinWorkers:     1,
			MaxWorkers:     16,
			InitialWorkers: 16,
		},
	}

	if err := eng.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
```

### Demo：用 (updated_at, id) 做增量（避免同一时间戳漏/重）

只用 `updated_at` 单字段做增量会遇到同一时间戳多行的问题，推荐用复合游标 `(updated_at, id)`：

```sql
WHERE updated_at > :ts OR (updated_at = :ts AND id > :id)
ORDER BY updated_at ASC, id ASC
```

可以通过自定义游标类型 + codec 实现：

```go
type Cursor struct {
	TS time.Time
	ID int64
}

type CursorCodec struct{}

func (CursorCodec) Encode(v Cursor) (string, error) {
	b, err := json.Marshal(map[string]any{
		"ts": v.TS.UTC().Format(time.RFC3339Nano),
		"id": v.ID,
	})
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (CursorCodec) Decode(s string) (Cursor, error) {
	if s == "" {
		return Cursor{}, nil
	}
	var m struct {
		TS string `json:"ts"`
		ID int64  `json:"id"`
	}
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return Cursor{}, err
	}
	ts, err := time.Parse(time.RFC3339Nano, m.TS)
	if err != nil {
		return Cursor{}, err
	}
	return Cursor{TS: ts, ID: m.ID}, nil
}

func (CursorCodec) Zero() Cursor { return Cursor{} }

build := func(ctx context.Context, c Cursor) (string, []any, error) {
	_ = ctx
	return `
SELECT id, updated_at, payload
FROM events
WHERE updated_at > ? OR (updated_at = ? AND id > ?)
ORDER BY updated_at ASC, id ASC
LIMIT 1000
`, []any{c.TS, c.TS, c.ID}, nil
}

extract := func(row map[string]any) (Cursor, error) {
	ts := row["updated_at"].(time.Time)
	id := row["id"].(int64)
	return Cursor{TS: ts, ID: id}, nil
}

src := &gormsource.SQLScanner[Cursor]{
	DB:            db,
	Store:         cp,
	CheckpointKey: "events",
	Codec:         CursorCodec{},
	BuildQuery:    build,
	ExtractCursor: extract,
}
```

## 断点续扫语义

- Source 会在每条消息上携带 `etl.Checkpoint{Key: CheckpointKey, Value: cursor}`。
- Engine 在 Sink 写入成功后才会 ack，并由内部 committer 在同一 partition 内按序提交到 `checkpoint.Store`。
- Source 重启后从 `checkpoint.Store.Load(CheckpointKey)` 读取上次游标，从 `cursor > last` 继续。

## 测试

断点续扫测试见：[sql_scanner_test.go](source/sql_scanner_test.go)

```bash
go test ./...
go vet ./...
```
