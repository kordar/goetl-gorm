package gormsource

import (
	"context"
	"errors"
	"time"

	"github.com/kordar/goetl"
)

func NewDBWalkerTicker(scanner *DBWalker, interval, retryInterval time.Duration, stopOnError bool) *DBWalkerTicker {
	return &DBWalkerTicker{
		Scanner:       scanner,
		Interval:      interval,
		RetryInterval: retryInterval,
		StopOnError:   stopOnError,
	}
}

type DBWalkerTicker struct {
	Scanner       *DBWalker
	Interval      time.Duration
	RetryInterval time.Duration
	StopOnError   bool
}

func (t *DBWalkerTicker) Name() string {
	if t.Scanner != nil {
		return t.Scanner.Name()
	}
	return "gorm_db_walker_ticker"
}

func (t *DBWalkerTicker) Start(ctx context.Context, out chan<- goetl.Message) error {
	if t.Scanner == nil {
		return errors.New("db walker ticker requires Scanner")
	}
	interval := t.Interval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	retry := t.RetryInterval
	if retry <= 0 {
		retry = interval
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := t.Scanner.Start(ctx, out)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			if t.StopOnError {
				return err
			}
			if sleepCtx(ctx, retry) != nil {
				return ctx.Err()
			}
			continue
		}

		if sleepCtx(ctx, interval) != nil {
			return ctx.Err()
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
