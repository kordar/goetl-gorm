# goetl-gorm

`goetl-gorm` 是 `goetl` 的 GORM 扩展包，提供可复用的数据库增量扫描 Source。

## 提供能力

- `source.SQLScanner[T]`：基于自定义 SQL 的通用扫描器
- `source.SQLScannerTicker[T]`：定时触发扫描包装器
- `transform.ModelToMap`：将 `Record.Data["model"]` 展开为 map（可选）
- `checkpoint.Store`：基于 GORM 的 checkpoint 存储实现

## 典型场景

- 按自增 ID 增量采集
- 按 `(updated_at, id)` 复合游标增量采集
- 定时轮询数据库并推送到 `goetl` pipeline

## 快速开始

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
		log.Fatal("init gorm db first")
	}

	cp := memory.NewCheckpointStore()
	src := &gormsource.SQLScanner[int64]{
		DB:            db,
		Store:         cp,
		CheckpointKey: "users",
		Codec:         gormsource.Int64CursorCodec{},
		BuildQuery: func(ctx context.Context, cursor int64) (string, []any, error) {
			_ = ctx
			return "SELECT id,name FROM users WHERE id > ? ORDER BY id LIMIT 1000", []any{cursor}, nil
		},
		ExtractCursor: func(row map[string]any) (int64, error) {
			switch v := row["id"].(type) {
			case int64:
				return v, nil
			case int:
				return int64(v), nil
			case string:
				return strconv.ParseInt(v, 10, 64)
			default:
				return 0, nil
			}
		},
	}

	eng := engine.NewEngine(
		/* your sink */,
		engine.WithPipeline(goetl.NewPipeline()),
		engine.WithCheckpoints(cp),
		engine.WithWorkers(1, 16, 4),
	)
	if err := eng.LoadSource("users", src); err != nil {
		log.Fatal(err)
	}
	if err := eng.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
```

## SQLScanner 核心参数

- `DB *gorm.DB`：数据库连接
- `Store checkpoint.Store`：checkpoint 存储（可选）
- `CheckpointKey string`：checkpoint key（必填）
- `Codec CursorCodec[T]`：游标编解码
- `BuildQuery`：根据当前游标返回 SQL 与参数
- `ExtractCursor`：从 row 提取下一游标
- `MapRow`：行映射（默认原样）

## 测试

```bash
go test ./...
go vet ./...
```
