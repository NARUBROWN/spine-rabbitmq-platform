package platform

import (
	"context"
	"errors"
	"maps"
	"net"
	"slices"
	"sync/atomic"

	eventadapter "github.com/NARUBROWN/spine/adapter/event"
	amqp "github.com/rabbitmq/amqp091-go"
)

type reader struct {
	messages <-chan amqp.Delivery
	wire     net.Conn
	topic    string
	config   Config
	stop     context.Context
	life     *lifecycle
	busy     atomic.Bool
	invalid  atomic.Bool
}

func newReader(messages <-chan amqp.Delivery, wire net.Conn, topic string, config Config, close func() error) *reader {
	stop, cancel := context.WithCancel(context.Background())
	r := &reader{messages: messages, wire: wire, topic: topic, config: config, stop: stop}
	r.life = newLifecycle(cancel, close)
	return r
}

func (r *reader) Read(ctx context.Context) (eventadapter.Delivery, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := r.life.enter(); err != nil {
		return nil, err
	}
	defer r.life.leave()
	if r.invalid.Load() {
		return nil, eventadapter.ErrReaderInvalidated
	}
	if !r.busy.CompareAndSwap(false, true) {
		return nil, eventadapter.ErrInvalidState
	}
	readCtx, cancel := mergeContext(ctx, r.stop)
	defer cancel()
	select {
	case <-readCtx.Done():
		r.busy.Store(false)
		r.invalid.Store(true)
		return nil, readCtx.Err()
	case native, ok := <-r.messages:
		if !ok {
			r.busy.Store(false)
			r.invalid.Store(true)
			return nil, errors.New("AMQP delivery channel closed")
		}
		metadata := make(map[string]string)
		for key, value := range native.Headers {
			if text, ok := value.(string); ok {
				metadata[key] = text
			}
		}
		name := native.Type
		if name == "" {
			name = r.topic
		}
		return &delivery{owner: r, native: native, message: eventadapter.Message{EventName: name,
			Payload: slices.Clone(native.Body), OccurredAt: native.Timestamp, Metadata: metadata}}, nil
	}
}

func (r *reader) Close(ctx context.Context) error { return r.life.close(ctx) }

type delivery struct {
	owner   *reader
	native  amqp.Delivery
	message eventadapter.Message
	settled atomic.Bool
}

func (d *delivery) Message() eventadapter.Message {
	message := d.message
	message.Payload = slices.Clone(message.Payload)
	message.Metadata = maps.Clone(message.Metadata)
	return message
}

func (d *delivery) Ack(ctx context.Context) error  { return d.settle(ctx, true) }
func (d *delivery) Nack(ctx context.Context) error { return d.settle(ctx, false) }

func (d *delivery) settle(ctx context.Context, success bool) error {
	if ctx == nil {
		return checkContext(ctx)
	}
	if err := d.owner.life.enter(); err != nil {
		return err
	}
	defer d.owner.life.leave()
	if !d.settled.CompareAndSwap(false, true) {
		return eventadapter.ErrInvalidState
	}
	defer d.owner.busy.Store(false)
	err := withIO(ctx, d.owner.wire, d.owner.config.OperationTimeout, func(context.Context) error {
		if success {
			return d.native.Ack(false)
		}
		return d.native.Nack(false, d.owner.config.FailurePolicy == FailureRequeue)
	})
	if err != nil {
		d.owner.invalid.Store(true)
	}
	return err
}
