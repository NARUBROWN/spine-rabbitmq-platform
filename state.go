package platform

import (
	"context"
	"fmt"
	"sync"
	"time"

	eventadapter "github.com/NARUBROWN/spine/contract/event"
)

// lifecycle는 새 작업 차단, 진행 작업 대기와 한 번의 자원 정리를 관리합니다.
type lifecycle struct {
	mu        sync.Mutex
	closing   bool
	active    sync.WaitGroup
	done      chan struct{}
	err       error
	interrupt func()
	cleanup   func() error
}

func newLifecycle(interrupt func(), cleanup func() error) *lifecycle {
	return &lifecycle{done: make(chan struct{}), interrupt: interrupt, cleanup: cleanup}
}

func (l *lifecycle) enter() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closing {
		return eventadapter.ErrInvalidState
	}
	l.active.Add(1)
	return nil
}

func (l *lifecycle) leave() { l.active.Done() }

func (l *lifecycle) close(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", eventadapter.ErrInvalidAdapter)
	}
	l.mu.Lock()
	if !l.closing {
		l.closing = true
		if l.interrupt != nil {
			l.interrupt()
		}
		go func() {
			l.active.Wait()
			l.err = l.cleanup()
			close(l.done)
		}()
	}
	l.mu.Unlock()
	select {
	case <-l.done:
		return l.err
	default:
	}
	select {
	case <-l.done:
		return l.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", eventadapter.ErrInvalidAdapter)
	}
	return contextError(ctx)
}

// 소켓 deadline이 context 타이머보다 먼저 깨어나도 원래 context 오류를 보존합니다.
func contextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

// mergeContext는 호출 context와 Reader의 종료 신호를 함께 반영합니다.
func mergeContext(ctx, stop context.Context) (context.Context, func()) {
	merged, cancel := context.WithCancel(ctx)
	unwatch := context.AfterFunc(stop, cancel)
	return merged, func() { unwatch(); cancel() }
}
