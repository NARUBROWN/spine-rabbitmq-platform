package platform

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	eventadapter "github.com/NARUBROWN/spine/contract/event"
	amqp "github.com/rabbitmq/amqp091-go"
)

type acknowledger struct {
	ack     int
	nack    int
	requeue bool
}

func (a *acknowledger) Ack(uint64, bool) error { a.ack++; return nil }
func (a *acknowledger) Nack(_ uint64, _ bool, requeue bool) error {
	a.nack++
	a.requeue = requeue
	return nil
}
func (a *acknowledger) Reject(uint64, bool) error { panic("unexpected reject") }

func TestReaderSettlesExactlyOnceWithConfiguredFailurePolicy(t *testing.T) {
	for _, policy := range []FailurePolicy{FailureReject, FailureRequeue} {
		t.Run(string(policy), func(t *testing.T) {
			wire, peer := net.Pipe()
			defer peer.Close()
			ack := &acknowledger{}
			messages := make(chan amqp.Delivery, 1)
			messages <- amqp.Delivery{Acknowledger: ack, DeliveryTag: 1, Type: "producer-controlled-name", Body: []byte("payload"), Headers: amqp.Table{"trace": "123"}}
			r := newReader(messages, wire, "registered-route", Config{FailurePolicy: policy, OperationTimeout: time.Second}, wire.Close)
			defer r.Close(context.Background())
			d, err := r.Read(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if d.Message().EventName != "producer-controlled-name" {
				t.Fatal("생산자 타입 메타데이터를 잃었습니다")
			}
			if _, err := r.Read(context.Background()); !errors.Is(err, eventadapter.ErrInvalidState) {
				t.Fatal(err)
			}
			if err := d.Nack(context.Background()); err != nil {
				t.Fatal(err)
			}
			if ack.ack != 0 || ack.nack != 1 || ack.requeue != (policy == FailureRequeue) {
				t.Fatalf("실패 정책이 다릅니다: %#v", ack)
			}
			if err := d.Ack(context.Background()); !errors.Is(err, eventadapter.ErrInvalidState) {
				t.Fatal(err)
			}
			if ack.ack != 0 {
				t.Fatal("중복 확인이 native ACK를 실행했습니다")
			}
		})
	}
}

func TestCanceledSettlementInvalidatesReader(t *testing.T) {
	wire, peer := net.Pipe()
	defer peer.Close()
	ack := &acknowledger{}
	messages := make(chan amqp.Delivery, 1)
	messages <- amqp.Delivery{Acknowledger: ack, DeliveryTag: 1}
	r := newReader(messages, wire, "orders", Config{OperationTimeout: time.Second}, wire.Close)
	defer r.Close(context.Background())
	d, err := r.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := d.Ack(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := d.Nack(context.Background()); !errors.Is(err, eventadapter.ErrInvalidState) {
		t.Fatal(err)
	}
	if _, err := r.Read(context.Background()); !errors.Is(err, eventadapter.ErrReaderInvalidated) {
		t.Fatal(err)
	}
	if ack.ack != 0 || ack.nack != 0 {
		t.Fatal("취소한 확인이 native 작업을 실행했습니다")
	}
}

type fakeConfirmation struct {
	done chan struct{}
	ack  bool
}

func (c *fakeConfirmation) Done() <-chan struct{} { return c.done }
func (c *fakeConfirmation) Acked() bool           { return c.ack }

type fakeSession struct {
	mu         sync.Mutex
	calls      int
	message    amqp.Publishing
	key        string
	returned   chan amqp.Return
	unroutable bool
	negative   bool
	wait       bool
	started    chan struct{}
}

func (s *fakeSession) Publish(_ string, key string, message amqp.Publishing) (confirmation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.message = message
	s.key = key
	if s.started != nil {
		close(s.started)
		s.started = nil
	}
	done := make(chan struct{})
	if !s.wait {
		close(done)
	}
	if s.unroutable {
		s.returned <- amqp.Return{ReplyCode: 312}
	}
	return &fakeConfirmation{done: done, ack: !s.negative}, nil
}
func (s *fakeSession) Returns() <-chan amqp.Return { return s.returned }

func TestPublisherRequiresConfirmAndDetectsMandatoryReturn(t *testing.T) {
	for _, mode := range []string{"success", "unroutable", "negative"} {
		t.Run(mode, func(t *testing.T) {
			wire, peer := net.Pipe()
			defer peer.Close()
			session := &fakeSession{returned: make(chan amqp.Return, 1), unroutable: mode == "unroutable", negative: mode == "negative"}
			p := newPublisher(session, wire, Config{Exchange: "events", TopicPrefix: "dev.", OperationTimeout: time.Second}, map[string]string{"orders": "dev.orders"}, wire.Close)
			defer p.Close(context.Background())
			message := eventadapter.Message{EventName: "orders", Payload: []byte{0, 255}, Metadata: map[string]string{"trace": "123"}}
			err := p.Publish(context.Background(), message)
			if mode == "success" && err != nil {
				t.Fatal(err)
			}
			if mode == "unroutable" && !errors.Is(err, ErrUnroutable) {
				t.Fatalf("mandatory return을 잃었습니다: %v", err)
			}
			if mode == "negative" && err == nil {
				t.Fatal("negative confirm을 성공으로 처리했습니다")
			}
			message.Payload[0] = 9
			message.Metadata["trace"] = "changed"
			if session.key != "dev.orders" || session.message.Body[0] != 0 || session.message.Headers["trace"] != "123" || session.message.DeliveryMode != amqp.Persistent {
				t.Fatalf("원본 본문과 메타데이터를 보존하지 않았습니다: %#v", session.message)
			}
		})
	}
}

func TestPublisherConcurrentWaitCanBeCanceled(t *testing.T) {
	wire, peer := net.Pipe()
	defer peer.Close()
	started := make(chan struct{})
	session := &fakeSession{returned: make(chan amqp.Return, 1), wait: true, started: started}
	p := newPublisher(session, wire, Config{Exchange: "events", OperationTimeout: time.Second}, map[string]string{"orders": "orders"}, func() error { _ = wire.Close(); return nil })
	defer p.Close(context.Background())
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- p.Publish(firstCtx, eventadapter.Message{EventName: "orders"}) }()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := p.Publish(ctx, eventadapter.Message{EventName: "orders"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("동시 발행 대기 취소가 막혔습니다: %v", err)
	}
	cancelFirst()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestContextCancellationInterruptsNativeWriteThatIgnoresContext(t *testing.T) {
	wire, peer := net.Pipe()
	defer peer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := withIO(ctx, wire, time.Second, func(context.Context) error { _, err := wire.Write([]byte("blocked")); return err })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("native 대기가 취소되지 않았습니다: %v", err)
	}
}

func TestFactoryValidatesWithoutConnectingAndDoesNotExposeCredentials(t *testing.T) {
	if _, err := NewFactory(Config{URL: "amqp://user:secret@localhost:5672", Exchange: "events", Consume: true}); !errors.Is(err, eventadapter.ErrInvalidAdapter) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("안전한 설정 검증이 아닙니다: %v", err)
	}
	dead := &DeadLetter{Exchange: "dlx"}
	f, err := NewFactory(Config{URL: "amqps://unreachable.invalid:5671", Exchange: "events", Consume: true, DeadLetter: dead})
	if err != nil {
		t.Fatal(err)
	}
	dead.Exchange = "changed"
	if f.(*factory).config.DeadLetter.Exchange != "dlx" {
		t.Fatal("설정이 원본 포인터를 공유합니다")
	}
	if err := eventadapter.ValidateFactory(f, eventadapter.Requirements{Consume: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.NewPublisher(context.Background()); !errors.Is(err, eventadapter.ErrUnsupportedCapability) {
		t.Fatal(err)
	}
}

func TestStartupCancellationUnblocksAMQPHandshake(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err == nil {
			defer conn.Close()
			buffer := make([]byte, 32)
			for {
				if _, err := conn.Read(buffer); err != nil {
					return
				}
			}
		}
	}()
	f, err := NewFactory(Config{URL: "amqp://guest:guest@" + listener.Addr().String(), Exchange: "events", Publish: true, PublishDestinations: testDestinations{"orders"}, AllowInsecureTransport: true, StartupTimeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	p, err := f.NewPublisher(context.Background())
	if p != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("AMQP handshake가 취소되지 않았습니다: %v", err)
	}
	<-done
}

type testDestinations []string

func (d testDestinations) PublishDestinations() []string { return d }
