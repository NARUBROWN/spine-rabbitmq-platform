English | [Korean](README.ko.md)

# Spine RabbitMQ Platform

A Go module connecting RabbitMQ to Spine's public event contract, `spine.event-adapter/v1`. It implements Factory, Reader, Delivery, and Publisher from `adapter/event`, without importing domain events, DI, or kernel internals.

## Preview installation

Use this channel to try features under development and check compatibility. The published tag is [`v0.2.0-preview.3`](https://github.com/NARUBROWN/spine-rabbitmq-platform/tree/v0.2.0-preview.3). Its `go.mod` specifies Spine `v0.7.0-preview.4` and Go 1.25.5 or later. The release policy on this branch targets `v0.2.0`.

```sh
go get github.com/NARUBROWN/spine-rabbitmq-platform@v0.2.0-preview.3
```

Other channels: [RC](https://github.com/NARUBROWN/spine-rabbitmq-platform/tree/rc) | [Stable](https://github.com/NARUBROWN/spine-rabbitmq-platform/tree/main).

Register the factory with `spine.WithEventAdapter(name, factory)`. The platform owns broker connections; the core owns consumer execution and reader recreation. The example below configures a worker without HTTP. HTTP applications also pass an Echo platform option to `spine.New`. It uses the public event contract and App integration API from Spine v0.6.5.

```go
import (
    "github.com/NARUBROWN/spine"
    rabbitmqplatform "github.com/NARUBROWN/spine-rabbitmq-platform"
)

func buildWorker() (spine.App, error) {
    factory, err := rabbitmqplatform.NewFactory(rabbitmqplatform.Config{URL: "amqps://rabbitmq.example.com:5671/", Exchange: "events", Consume: true, Publish: true, PublishDestinations: destinations.Catalog{}})
    if err != nil { return nil, err }
    return spine.New(spine.WithEventAdapter("rabbitmq", factory)), nil
}
```

Register consumer constructors and handlers with `app.Consumers().Register`, then run `app.Run(boot.Options{})`. Configure common policies with `spine.WithEventConsumerRetry` and `spine.WithEventSettlementTimeout`. Prepare retains readers, and Run starts consumption through the same preparation path. See the [App migration guide](https://github.com/NARUBROWN/spine/blob/5d8ea93/docs/migration/event-platforms.md).

## Usage

```go
import (
    "context"

    eventadapter "github.com/NARUBROWN/spine/contract/event"
    rabbitmqplatform "github.com/NARUBROWN/spine-rabbitmq-platform"
)

func consumeOne(ctx context.Context) error {
    factory, err := rabbitmqplatform.NewFactory(rabbitmqplatform.Config{
        URL: "amqp://guest:guest@localhost:5672/",
        Exchange: "events",
        Consume: true,
        FailurePolicy: rabbitmqplatform.FailureReject,
        AllowInsecureTransport: true, // Use only with an isolated local development broker.
    })
    if err != nil { return err }
    reader, err := factory.NewReader(ctx, eventadapter.Subscription{Topic: "order.created"})
    if err != nil { return err }
    defer reader.Close(context.Background())
    delivery, err := reader.Read(ctx)
    if err != nil { return err }
    // Process delivery.Message(), then call Ack on success or Nack on failure.
    return delivery.Ack(ctx)
}
```

Publish with `Config.Publish: true`, `Config.PublishDestinations`, and `factory.NewPublisher(ctx)`. Pass an already serialized body to `Publish(ctx, eventadapter.Message{EventName: ..., Payload: ...})`. The platform neither assumes a body format nor serializes it again.

When enabling publishing, configure a value implementing `PublishDestinations() []string`. The publisher prepares the existing topic exchange and uses registered names with the prefix as routing keys. Without a matching consumer queue binding, mandatory publishing returns `ErrUnroutable`.

```go
// destinations.go
package destinations

const OrderCreated = "orders.created"

type Catalog struct{}

func (Catalog) PublishDestinations() []string {
    return []string{OrderCreated}
}
```

## Configuration

| Field | Behavior |
| --- | --- |
| `URL` | AMQP connection string; requires `amqps://` by default |
| `Exchange` | Required durable topic exchange name; the default exchange is unsupported |
| `Consume`, `Publish` | Enable reading and publishing independently; enable at least one |
| `PublishDestinations` | Required provider of logical destinations when publishing |
| `QueuePrefix` | Queue name prefix for logical subscription keys; empty by default |
| `TopicPrefix` | Adds the same prefix to published names and subscription binding keys |
| `PrefetchCount` | Defaults to 1. Positive values are allowed, but each Reader hands only one delivery at a time to the kernel. |
| `FailurePolicy` | Defaults to `FailureReject`; `FailureRequeue` requeues failed messages |
| `DeadLetter` | Optional dead-letter exchange and routing key. Callers provision the DLX, destination queue, and bindings. |
| `TLS` | Copies the TLS configuration; defaults to TLS 1.2 or later |
| `AllowInsecureTransport` | Defaults to false. Explicitly permits local plaintext connections; cannot be combined with TLS settings. |
| `StartupTimeout` | Connection, exchange, queue, and binding readiness timeout; defaults to 10 seconds |
| `OperationTimeout` | Settlement, publishing, and publisher confirm timeout; defaults to 5 seconds |

Factory creation and Capabilities queries do not access the network. NewReader connects, declares the durable exchange and queue, creates bindings, applies QoS, and starts the consumer subscription. NewPublisher declares the exchange and enables confirms. The creation context only bounds initialization; successful resources remain alive until Close.

## Processing and resource lifetime

- Each Reader has one unsettled delivery. Ack acknowledges one message; Nack rejects or requeues it according to configuration. Duplicate settlement does not repeat native operations. After a settlement error, close and recreate the Reader.
- The kernel runs the handler selected by `Subscription.Topic`. AMQP Type is preserved in Message.EventName but does not select the handler. Only string AMQP headers are copied to Metadata; other native header types are omitted.
- Publishing uses persistent messages, `mandatory=true`, and publisher confirms. A missing receiving queue returns `ErrUnroutable`; negative confirms and confirmation timeouts return errors. Concurrent publish calls are serialized within each native session.
- Transport or settlement errors that leave a session uncertain block further work on that resource. Create a new Publisher before retrying. There is no automatic republishing. The broker may have received a message before an error or cancellation, so the application must handle duplicates.
- Native send methods ignore context; the platform closes the socket on cancellation or timeout to release the wait. Settlement outcomes may then be uncertain.
- Close blocks new work, waits for in-flight settlement and publishing, and cleans up the connection once. The context only bounds waiting; later Close calls can wait again. Normal native connection shutdown is limited to 5 seconds.
- Retention of rejected messages depends on the DLX topology. Without a DLX, messages may be discarded. Requeue can repeatedly redeliver messages that fail permanently.

## Verification

```sh
go test -race ./... -count=1
bash scripts/check-module-boundary.sh
SPINE_TEST_RABBITMQ_URL=amqp://guest:guest@localhost:5672/ go test -race . -run '^TestLive' -count=1 -timeout=2m
```

Live tests are skipped when broker environment variables are absent. They check binary bodies, header round trips, mandatory returns, publisher confirms, requeue redelivery, and dead-letter retention after rejection. CI runs unit, race, and RabbitMQ live checks separately. Production TLS, authentication, and cluster failures require separate validation.

License: [MIT](LICENSE). Contract: [Spine adapter/event](https://github.com/NARUBROWN/spine/tree/1889d66/adapter/event).

## v0.1.0 compatibility

This release requires Spine `v0.6.5` or later, which includes the public contracts. Install the official tag without a `replace` pointing to a private path.

```sh
go get github.com/NARUBROWN/spine-rabbitmq-platform@v0.1.0
```

## Releases and public contracts

This platform publishes its own versions and release channels. The Spine version in `go.mod` is the minimum dependency for its public contracts and is managed separately from the core release channel. CI and public installation checks verify the platform against this module's dependencies. Core releases verify combinations of a new core and existing platform tags.

The next publication target is `v0.3.0`.
