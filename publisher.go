package platform

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"sync/atomic"

	eventadapter "github.com/NARUBROWN/spine/contract/event"
	amqp "github.com/rabbitmq/amqp091-go"
)

// ErrUnroutable은 mandatory 발행을 수신할 큐가 없음을 나타냅니다.
var ErrUnroutable = errors.New("RabbitMQ message was unroutable")

type confirmation interface {
	Done() <-chan struct{}
	Acked() bool
}
type publishSession interface {
	Publish(string, string, amqp.Publishing) (confirmation, error)
	Returns() <-chan amqp.Return
}

type nativeSession struct {
	ch       *amqp.Channel
	returned <-chan amqp.Return
}

func (s *nativeSession) Publish(exchange, key string, message amqp.Publishing) (confirmation, error) {
	return s.ch.PublishWithDeferredConfirm(exchange, key, true, false, message)
}
func (s *nativeSession) Returns() <-chan amqp.Return { return s.returned }

type publisher struct {
	session      publishSession
	wire         net.Conn
	config       Config
	destinations map[string]string
	serial       chan struct{}
	invalid      atomic.Bool
	life         *lifecycle
}

func newPublisher(session publishSession, wire net.Conn, config Config, destinations map[string]string, close func() error) *publisher {
	p := &publisher{session: session, wire: wire, config: config, destinations: destinations, serial: make(chan struct{}, 1)}
	p.life = newLifecycle(nil, close)
	return p
}

func (p *publisher) Publish(ctx context.Context, message eventadapter.Message) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := p.life.enter(); err != nil {
		return err
	}
	defer p.life.leave()
	routingKey, registered := p.destinations[message.EventName]
	if !registered {
		return fmt.Errorf("%w: RabbitMQ publish destination %q is not registered", eventadapter.ErrInvalidAdapter, message.EventName)
	}
	select {
	case p.serial <- struct{}{}:
		defer func() { <-p.serial }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if p.invalid.Load() {
		return eventadapter.ErrInvalidState
	}
	headers := make(amqp.Table, len(message.Metadata))
	for key, value := range message.Metadata {
		headers[key] = value
	}
	native := amqp.Publishing{DeliveryMode: amqp.Persistent, Body: slices.Clone(message.Payload),
		Timestamp: message.OccurredAt, Type: message.EventName, Headers: headers}
	err := withIO(ctx, p.wire, p.config.OperationTimeout, func(operationCtx context.Context) error {
		confirmed, err := p.session.Publish(p.config.Exchange, routingKey, native)
		if err != nil {
			return err
		}
		if confirmed == nil {
			return errors.New("AMQP publisher confirm is disabled")
		}
		returns := p.session.Returns()
		var returned *amqp.Return
		for {
			select {
			case value, ok := <-returns:
				if !ok {
					returns = nil
				} else {
					returned = &value
				}
			case <-confirmed.Done():
				// broker는 basic.return을 confirm보다 먼저 전송합니다. 버퍼도 한 번 확인합니다.
				if returned == nil && returns != nil {
					select {
					case value, ok := <-returns:
						if ok {
							returned = &value
						}
					default:
					}
				}
				if returned != nil {
					return fmt.Errorf("%w: reply_code=%d", ErrUnroutable, returned.ReplyCode)
				}
				if !confirmed.Acked() {
					return errors.New("AMQP broker rejected publish")
				}
				return nil
			case <-operationCtx.Done():
				return operationCtx.Err()
			}
		}
	})
	if err != nil && !errors.Is(err, ErrUnroutable) {
		p.invalid.Store(true)
		_ = p.wire.Close()
	}
	return err
}

func (p *publisher) Close(ctx context.Context) error { return p.life.close(ctx) }
