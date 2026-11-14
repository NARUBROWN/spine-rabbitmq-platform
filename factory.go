// Package platform은 Spine 이벤트 계약의 RabbitMQ 구현입니다.
package platform

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"reflect"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	eventadapter "github.com/NARUBROWN/spine/contract/event"
	amqp "github.com/rabbitmq/amqp091-go"
)

type FailurePolicy string

const (
	FailureReject  FailurePolicy = "reject"
	FailureRequeue FailurePolicy = "requeue"
)

type DeadLetter struct {
	Exchange   string
	RoutingKey string
}

// DestinationSource는 앱이 발행할 논리 목적지 목록을 제공합니다.
type DestinationSource interface {
	PublishDestinations() []string
}

// Config는 durable topic exchange의 송수신과 실패 정책을 설정합니다.
type Config struct {
	URL                    string
	Exchange               string
	Consume                bool
	Publish                bool
	QueuePrefix            string
	TopicPrefix            string
	PublishDestinations    DestinationSource
	PrefetchCount          int
	FailurePolicy          FailurePolicy
	DeadLetter             *DeadLetter
	TLS                    *tls.Config
	AllowInsecureTransport bool
	StartupTimeout         time.Duration
	OperationTimeout       time.Duration
}

type factory struct {
	config       Config
	destinations map[string]string
}

// NewFactory는 설정만 검사하고 복사하며 브로커에 연결하지 않습니다.
func NewFactory(config Config) (eventadapter.Factory, error) {
	parsed, err := url.Parse(config.URL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "amqp" && parsed.Scheme != "amqps") {
		return nil, fmt.Errorf("%w: invalid AMQP URL", eventadapter.ErrInvalidAdapter)
	}
	if (parsed.Scheme == "amqp" && !config.AllowInsecureTransport) ||
		(config.AllowInsecureTransport && (config.TLS != nil || parsed.Scheme == "amqps")) {
		return nil, fmt.Errorf("%w: configure AMQPS or explicitly allow plaintext transport", eventadapter.ErrInvalidAdapter)
	}
	if config.Exchange == "" || (!config.Consume && !config.Publish) || config.PrefetchCount < 0 || config.StartupTimeout < 0 || config.OperationTimeout < 0 {
		return nil, fmt.Errorf("%w: exchange, roles, prefetch and timeouts must be valid", eventadapter.ErrInvalidAdapter)
	}
	if config.FailurePolicy == "" {
		config.FailurePolicy = FailureReject
	}
	if config.FailurePolicy != FailureReject && config.FailurePolicy != FailureRequeue {
		return nil, fmt.Errorf("%w: invalid failure policy", eventadapter.ErrInvalidAdapter)
	}
	if config.DeadLetter != nil {
		if config.DeadLetter.Exchange == "" {
			return nil, fmt.Errorf("%w: dead-letter exchange is empty", eventadapter.ErrInvalidAdapter)
		}
		copy := *config.DeadLetter
		config.DeadLetter = &copy
	}
	if config.TLS != nil {
		config.TLS = config.TLS.Clone()
	} else {
		config.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if config.PrefetchCount == 0 {
		config.PrefetchCount = 1
	}
	if config.StartupTimeout == 0 {
		config.StartupTimeout = 10 * time.Second
	}
	if config.OperationTimeout == 0 {
		config.OperationTimeout = 5 * time.Second
	}
	destinations, err := resolveDestinations(config)
	if err != nil {
		return nil, err
	}
	// 초기화 후에는 호출자가 목록을 바꿔도 발행 경로에 영향을 주지 않습니다.
	config.PublishDestinations = nil
	return &factory{config: config, destinations: destinations}, nil
}

func (f *factory) Capabilities() eventadapter.Capabilities {
	return eventadapter.Capabilities{ContractVersion: eventadapter.ContractVersion, Consume: f.config.Consume, Publish: f.config.Publish}
}

// connect는 AMQP 초기화 전체가 취소될 수 있도록 raw 연결의 소유권도 보관합니다.
func (f *factory) connect(ctx context.Context) (*amqp.Connection, net.Conn, func(), error) {
	var wire net.Conn
	var unwatch func() bool
	conn, err := amqp.DialConfig(f.config.URL, amqp.Config{TLSClientConfig: f.config.TLS.Clone(), Dial: func(network, address string) (net.Conn, error) {
		var err error
		wire, err = (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		if deadline, ok := ctx.Deadline(); ok {
			if err = wire.SetDeadline(deadline); err != nil {
				_ = wire.Close()
				return nil, err
			}
		}
		unwatch = context.AfterFunc(ctx, func() { _ = wire.Close() })
		return wire, nil
	}})
	release := func() {
		if unwatch != nil {
			unwatch()
		}
		if wire != nil {
			_ = wire.SetDeadline(time.Time{})
		}
	}
	if err != nil {
		release()
		if wire != nil {
			_ = wire.Close()
		}
		if contextErr := contextError(ctx); contextErr != nil {
			return nil, nil, nil, fmt.Errorf("AMQP startup: %w", errors.Join(err, contextErr))
		}
		return nil, nil, nil, err
	}
	return conn, wire, release, nil
}

func closeConnection(conn *amqp.Connection, wire net.Conn) error {
	err := conn.CloseDeadline(time.Now().Add(5 * time.Second))
	_ = wire.Close()
	if errors.Is(err, amqp.ErrClosed) {
		return nil
	}
	return err
}

func (f *factory) NewReader(ctx context.Context, subscription eventadapter.Subscription) (eventadapter.Reader, error) {
	if !f.config.Consume {
		return nil, eventadapter.ErrUnsupportedCapability
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if subscription.Topic == "" {
		return nil, fmt.Errorf("%w: subscription topic is empty", eventadapter.ErrInvalidAdapter)
	}
	initCtx, cancel := context.WithTimeout(ctx, f.config.StartupTimeout)
	defer cancel()
	conn, wire, release, err := f.connect(initCtx)
	if err != nil {
		return nil, err
	}
	defer release()
	ready := false
	defer func() {
		if !ready {
			_ = wire.Close()
			_ = closeConnection(conn, wire)
		}
	}()
	ch, err := conn.Channel()
	if err == nil {
		err = ch.ExchangeDeclare(f.config.Exchange, "topic", true, false, false, false, nil)
	}
	queue := f.config.QueuePrefix + subscription.Topic
	var args amqp.Table
	if f.config.DeadLetter != nil {
		args = amqp.Table{"x-dead-letter-exchange": f.config.DeadLetter.Exchange}
		if f.config.DeadLetter.RoutingKey != "" {
			args["x-dead-letter-routing-key"] = f.config.DeadLetter.RoutingKey
		}
	}
	if err == nil {
		_, err = ch.QueueDeclare(queue, true, false, false, false, args)
	}
	if err == nil {
		err = ch.QueueBind(queue, f.config.TopicPrefix+subscription.Topic, f.config.Exchange, false, nil)
	}
	if err == nil {
		err = ch.Qos(f.config.PrefetchCount, 0, false)
	}
	var messages <-chan amqp.Delivery
	if err == nil {
		messages, err = ch.Consume(queue, "", false, false, false, false, nil)
	}
	release()
	if initCtx.Err() != nil {
		return nil, initCtx.Err()
	}
	if err != nil {
		return nil, err
	}
	r := newReader(messages, wire, subscription.Topic, f.config, func() error { return closeConnection(conn, wire) })
	ready = true
	return r, nil
}

func (f *factory) NewPublisher(ctx context.Context) (eventadapter.Publisher, error) {
	if !f.config.Publish {
		return nil, eventadapter.ErrUnsupportedCapability
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	initCtx, cancel := context.WithTimeout(ctx, f.config.StartupTimeout)
	defer cancel()
	conn, wire, release, err := f.connect(initCtx)
	if err != nil {
		return nil, err
	}
	defer release()
	ready := false
	defer func() {
		if !ready {
			_ = wire.Close()
			_ = closeConnection(conn, wire)
		}
	}()
	ch, err := conn.Channel()
	if err == nil {
		err = ch.ExchangeDeclare(f.config.Exchange, "topic", true, false, false, false, nil)
	}
	if err == nil {
		err = ch.Confirm(false)
	}
	release()
	if initCtx.Err() != nil {
		return nil, initCtx.Err()
	}
	if err != nil {
		return nil, err
	}
	p := newPublisher(&nativeSession{ch: ch, returned: ch.NotifyReturn(make(chan amqp.Return, 1))}, wire,
		f.config, f.destinations, func() error { return closeConnection(conn, wire) })
	ready = true
	return p, nil
}

// withIO는 context를 무시하는 native 송신도 취소 시 연결을 닫아 해제합니다.
func withIO(ctx context.Context, wire net.Conn, timeout time.Duration, operation func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline, _ := ctx.Deadline()
	if err := wire.SetWriteDeadline(deadline); err != nil {
		return err
	}
	unwatch := context.AfterFunc(ctx, func() { _ = wire.Close() })
	err := operation(ctx)
	unwatch()
	if contextError(ctx) != nil {
		_ = wire.Close()
		return errors.Join(err, contextError(ctx))
	}
	clearErr := wire.SetWriteDeadline(time.Time{})
	return errors.Join(err, clearErr)
}

func resolveDestinations(config Config) (map[string]string, error) {
	if !config.Publish {
		return nil, nil
	}
	source := config.PublishDestinations
	if source == nil || nilDestinationSource(source) {
		return nil, fmt.Errorf("%w: set Config.PublishDestinations when publishing is enabled", eventadapter.ErrInvalidAdapter)
	}
	names := source.PublishDestinations()
	if len(names) == 0 {
		return nil, fmt.Errorf("%w: Config.PublishDestinations returned no destinations", eventadapter.ErrInvalidAdapter)
	}
	destinations := make(map[string]string, len(names))
	for _, name := range names {
		address := config.TopicPrefix + name
		if strings.TrimSpace(name) == "" || !utf8.ValidString(address) || len(address) > 255 || strings.IndexFunc(address, unicode.IsControl) >= 0 {
			return nil, fmt.Errorf("%w: invalid rabbitmq destination %q", eventadapter.ErrInvalidAdapter, name)
		}
		if _, exists := destinations[name]; exists {
			return nil, fmt.Errorf("%w: duplicate rabbitmq destination %q", eventadapter.ErrInvalidAdapter, name)
		}
		destinations[name] = address
	}
	return destinations, nil
}

func nilDestinationSource(source DestinationSource) bool {
	value := reflect.ValueOf(source)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
