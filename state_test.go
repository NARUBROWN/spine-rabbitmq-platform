package platform

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	eventadapter "github.com/NARUBROWN/spine/adapter/event"
)

func TestCloseWaitsOnceAndPreservesCleanupResult(t *testing.T) {
	var count atomic.Int32
	cause := errors.New("cleanup failed")
	life := newLifecycle(nil, func() error { count.Add(1); return cause })
	if err := life.enter(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := life.close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("종료 대기는 context를 따라야 합니다: %v", err)
	}
	if count.Load() != 0 {
		t.Fatal("진행 작업이 끝나기 전에 정리했습니다")
	}
	if err := life.enter(); !errors.Is(err, eventadapter.ErrInvalidState) {
		t.Fatalf("종료 중 새 작업을 허용했습니다: %v", err)
	}
	life.leave()
	var wait sync.WaitGroup
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if err := life.close(context.Background()); !errors.Is(err, cause) {
				t.Errorf("정리 오류가 보존되지 않았습니다: %v", err)
			}
		}()
	}
	wait.Wait()
	if count.Load() != 1 {
		t.Fatalf("정리 횟수 = %d", count.Load())
	}
}
