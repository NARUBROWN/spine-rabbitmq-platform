package platform_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	platform "github.com/NARUBROWN/spine-rabbitmq-platform"
	eventadapter "github.com/NARUBROWN/spine/adapter/event"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestLiveRabbitMQConfirmRequeueAndDeadLetter(t *testing.T) {
	address := os.Getenv("SPINE_TEST_RABBITMQ_URL")
	if address == "" {
		t.Skip("SPINE_TEST_RABBITMQ_URL is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	name := fmt.Sprintf("spine-platform-%d", time.Now().UnixNano())
	conn, err := amqp.Dial(address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	dlx := name + ".dlx"
	dlq := name + ".dlq"
	if err := ch.ExchangeDeclare(dlx, "topic", true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	defer ch.ExchangeDelete(dlx, false, false)
	if _, err := ch.QueueDeclare(dlq, true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	defer ch.QueueDelete(dlq, false, false, false)
	if err := ch.QueueBind(dlq, "dead", dlx, false, nil); err != nil {
		t.Fatal(err)
	}
	config := platform.Config{URL: address, Exchange: name, Consume: true, Publish: true, QueuePrefix: name + ".", TopicPrefix: "route.", FailurePolicy: platform.FailureRequeue, DeadLetter: &platform.DeadLetter{Exchange: dlx, RoutingKey: "dead"}, AllowInsecureTransport: true}
	f, err := platform.NewFactory(config)
	if err != nil {
		t.Fatal(err)
	}
	initCtx, cancelInit := context.WithCancel(ctx)
	r, err := f.NewReader(initCtx, eventadapter.Subscription{Topic: "orders"})
	cancelInit()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.QueueDelete(name+".orders", false, false, false)
	defer ch.ExchangeDelete(name, false, false)
	p, err := f.NewPublisher(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close(context.Background())
	if err := p.Publish(ctx, eventadapter.Message{EventName: "missing", Payload: []byte("unroutable")}); !errors.Is(err, platform.ErrUnroutable) {
		t.Fatalf("mandatory return을 받지 못했습니다: %v", err)
	}
	payload := []byte{0, 255, 1}
	if err := p.Publish(ctx, eventadapter.Message{EventName: "orders", Payload: payload, Metadata: map[string]string{"trace": "123"}}); err != nil {
		t.Fatal(err)
	}
	d, err := r.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(d.Message().Payload, payload) || d.Message().Metadata["trace"] != "123" {
		t.Fatalf("메시지 왕복이 다릅니다: %#v", d.Message())
	}
	if err := d.Nack(ctx); err != nil {
		t.Fatal(err)
	}
	d, err = r.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(d.Message().Payload, payload) {
		t.Fatal("재큐잉한 메시지가 다릅니다")
	}
	if err := d.Ack(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(ctx); err != nil {
		t.Fatal(err)
	}
	config.FailurePolicy = platform.FailureReject
	f, err = platform.NewFactory(config)
	if err != nil {
		t.Fatal(err)
	}
	r, err = f.NewReader(ctx, eventadapter.Subscription{Topic: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())
	if err := p.Publish(ctx, eventadapter.Message{EventName: "orders", Payload: []byte("reject")}); err != nil {
		t.Fatal(err)
	}
	d, err = r.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Nack(ctx); err != nil {
		t.Fatal(err)
	}
	for {
		message, ok, err := ch.Get(dlq, false)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			if string(message.Body) != "reject" {
				t.Fatalf("dead-letter 본문이 다릅니다: %q", message.Body)
			}
			if err := message.Ack(false); err != nil {
				t.Fatal(err)
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}
