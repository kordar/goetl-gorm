# goetl-gorm

基于 GORM/sql 的增量扫描 Source 组件与数据库 Checkpoint 存储，配合 [goetl](https://github.com/kordar/goetl) 引擎进行表数据的批量/持续拉取与分发。

## 功能概览

- Source
  - DBWalker：一次性分页扫描（按游标推进），本轮无数据即返回
  - DBWalkerTicker：定时执行 DBWalker，每个周期跑一轮（适合持续增量）
- Checkpoint
  - checkpoint.Store（GORM）：以表存储 `checkpoint.Cursor`，支持断点恢复

## 安装

```bash
go get github.com/kordar/goetl-gorm@v0.1.0
```

## 快速开始

每 1 秒按 id 递增拉取一条记录，拉取到的消息交由 Sink 批量处理。

```go
package main

import (
    "context"
    "database/sql"
    "fmt"
    "time"

    _ "github.com/go-sql-driver/mysql"
    "github.com/kordar/goetl"
    "github.com/kordar/goetl/checkpoint"
    sinkdispatcher "github.com/kordar/goetl/dispatcher/sink"
    "github.com/kordar/goetl/engine"
    gormsource "github.com/kordar/goetl-gorm/source"
)

func main() {
    db, _ := sql.Open("mysql", "user:pass@tcp(127.0.0.1:3306)/demo?parseTime=true")
    defer db.Close()

    query := "SELECT id, name FROM t WHERE id > ? ORDER BY id LIMIT ?"

    w := &gormsource.DBWalker{
        SQL:           db,
        CheckpointKey: "t:id",
        PageSize:      1, // 每页大小
        MaxItems:      1, // 每轮最多处理条数（让每秒只拉1条）
        BuildQuery: func(ctx context.Context, cur checkpoint.Cursor, limit int) (string, []any, error) {
            last := int64(0)
            if len(cur.Values) > 0 {
                if v, ok := cur.Values[0].(int64); ok { last = v }
            }
            return query, []any{last, limit}, nil
        },
        ExtractCursor: func(row map[string]any) (checkpoint.Cursor, error) {
            id := row["id"].(int64)
            return checkpoint.Cursor{Values: []any{id}}, nil
        },
        MapRow: func(row map[string]any) (any, error) { return row, nil },
        BuildAck: func(row map[string]any, cur checkpoint.Cursor) (string, error) {
            return fmt.Sprintf("t:%d", row["id"].(int64)), nil
        },
    }

    tk := gormsource.NewDBWalkerTicker(w, time.Second, time.Second, true)

    sink := &printSink{}
    d := sinkdispatcher.NewBatchSinkDispatcher(sink).
        WithBatchSize(1).
        WithFlushInterval(100 * time.Millisecond).
        WithQueueBuffer(128)

    eng := engine.NewEngine().WithSource(tk).WithDispatcher(d)
    ctx, cancel := context.WithCancel(context.Background())
    eng.Run(ctx, func(m goetl.Message) {}, func(err error) { fmt.Println("err:", err) })

    time.Sleep(5 * time.Second)
    cancel()
    eng.Stop()
}

type printSink struct{}

func (s *printSink) Name() string { return "print" }
func (s *printSink) WriteBatch(ctx context.Context, msgs []goetl.Message) error {
    for _, m := range msgs {
        fmt.Println("record:", m.Record.Data, "cursor:", m.Cursor.Values)
    }
    return nil
}
```

> 提示：如需跨轮增量，请为 DBWalker 配置 `Store checkpoint.CheckpointStore`，并在引擎侧通过 RuntimeFactory 配置保存策略；这样下一轮会从上次游标继续拉取。

## Checkpoint（GORM）

参见 `checkpoint/store.go`，表结构：

- namespace（主键）
- checkpoint_key（主键）
- cursor_json（保存 `checkpoint.Cursor` 的 JSON）
- updated_at

在保存游标时建议将 `cursor.Meta["checkpoint"]` 设置为 `CheckpointKey` 以便统一提交。

## 测试

```bash
go test ./...
```

## 许可证

MIT

