# Spine RabbitMQ Platform

RabbitMQ를 Spine의 공개 이벤트 계약 `spine.event-adapter/v1`에 연결하는 Go 모듈입니다. `adapter/event`의 Factory, Reader, Delivery와 Publisher를 구현하며 도메인 이벤트, DI나 커널 내부 패키지를 import하지 않습니다.

App에는 `spine.WithEventAdapter(name, factory)`로 등록합니다. 브로커 연결은 이 플랫폼이, 소비 실행과 reader 재연결은 코어가 맡습니다. 아래는 HTTP 없는 워커 구성입니다. HTTP 앱은 `spine.New`에 Echo 플랫폼 옵션도 함께 넘깁니다. 초기 개발 버전이며 App 연결을 포함한 Spine 개발 커밋을 go.mod에 고정합니다.

```go
import (
    "github.com/NARUBROWN/spine"
    rabbitmqplatform "github.com/NARUBROWN/spine-rabbitmq-platform"
)

func buildWorker() (spine.App, error) {
    factory, err := rabbitmqplatform.NewFactory(rabbitmqplatform.Config{URL: "amqps://rabbitmq.example.com:5671/", Exchange: "events", Consume: true, Publish: true})
    if err != nil { return nil, err }
    return spine.New(spine.WithEventAdapter("rabbitmq", factory)), nil
}
```

기존 consumer 생성자와 `app.Consumers().Register`를 등록한 뒤 `app.Run(boot.Options{})`로 실행합니다. 공통 정책은 `spine.WithEventConsumerRetry`와 `spine.WithEventSettlementTimeout`으로 조정합니다. Prepare는 reader를 보관하고 Run이 같은 준비 경로에서 소비를 시작합니다. [App 전환 안내](https://github.com/NARUBROWN/spine/blob/5d8ea93/docs/migration/event-platforms.md)를 참고하세요.

## 사용

```go
import (
    "context"

    eventadapter "github.com/NARUBROWN/spine/adapter/event"
    rabbitmqplatform "github.com/NARUBROWN/spine-rabbitmq-platform"
)

func consumeOne(ctx context.Context) error {
    factory, err := rabbitmqplatform.NewFactory(rabbitmqplatform.Config{
        URL: "amqp://guest:guest@localhost:5672/",
        Exchange: "events",
        Consume: true,
        FailurePolicy: rabbitmqplatform.FailureReject,
        AllowInsecureTransport: true, // 격리된 로컬 개발 브로커에서만 사용합니다.
    })
    if err != nil { return err }
    reader, err := factory.NewReader(ctx, eventadapter.Subscription{Topic: "order.created"})
    if err != nil { return err }
    defer reader.Close(context.Background())
    delivery, err := reader.Read(ctx)
    if err != nil { return err }
    // delivery.Message()의 본문을 처리한 뒤 성공하면 Ack, 실패하면 Nack을 호출합니다.
    return delivery.Ack(ctx)
}
```

발행은 `Config.Publish: true`와 `factory.NewPublisher(ctx)`를 사용합니다. `Publish(ctx, eventadapter.Message{EventName: ..., Payload: ...})`에는 이미 직렬화한 본문을 넘깁니다. 본문 형식을 가정하거나 다시 직렬화하지 않습니다.

## 설정

| 필드 | 동작 |
| --- | --- |
| `URL` | AMQP 연결 문자열. 기본적으로 `amqps://`가 필요합니다. |
| `Exchange` | durable topic exchange 이름. 필수이며 기본 exchange는 지원하지 않습니다. |
| `Consume`, `Publish` | 읽기·발행 역할을 각각 활성화합니다. 하나 이상 필요합니다. |
| `QueuePrefix` | 논리 구독 키 앞에 붙일 큐 이름 접두사. 기본은 빈 문자열입니다. |
| `TopicPrefix` | 발행 이름과 구독 binding key 앞에 같은 접두사를 붙입니다. |
| `PrefetchCount` | 기본 1. 양수로 지정할 수 있지만 Reader는 한 번에 하나의 전달만 커널에 넘깁니다. |
| `FailurePolicy` | 기본 `FailureReject`. `FailureRequeue`는 실패 메시지를 재큐잉합니다. |
| `DeadLetter` | 선택적 dead-letter exchange와 routing key. DLX·대상 큐·binding은 호출자가 준비합니다. |
| `TLS` | TLS 설정을 복사합니다. 기본값은 TLS 1.2 이상입니다. |
| `AllowInsecureTransport` | 기본 false. 로컬 평문 연결을 명시적으로 허용하며 TLS 설정과 함께 사용할 수 없습니다. |
| `StartupTimeout` | 연결·exchange·queue·binding 준비 제한 시간. 기본 10초입니다. |
| `OperationTimeout` | 확인·발행과 publisher confirm 대기 제한 시간. 기본 5초입니다. |

factory 생성과 Capabilities 조회는 네트워크를 사용하지 않습니다. NewReader는 연결, durable exchange·queue 선언, binding, QoS와 consumer 구독을 완료합니다. NewPublisher는 exchange 선언과 confirm 설정을 완료합니다. 생성 context는 초기화만 제한하고 성공한 자원은 Close까지 유지됩니다.

## 처리와 수명

- 한 Reader에서 미확인 전달은 하나입니다. Ack는 한 건만 확인하고 Nack은 설정에 따라 reject 또는 requeue합니다. 같은 전달의 중복 확인은 native 작업을 반복하지 않습니다. 확인 오류 뒤에는 Reader를 닫고 다시 생성해야 합니다.
- 커널은 `Subscription.Topic`에 연결된 핸들러를 실행해야 합니다. AMQP Type은 Message.EventName에 보존하지만 핸들러 선택에 사용하지 않습니다. 문자열 AMQP headers만 Metadata에 옮기며 그 밖의 native header 타입은 생략합니다.
- 발행은 persistent 메시지, `mandatory=true`와 publisher confirm을 사용합니다. 수신 큐가 없으면 `ErrUnroutable`, negative confirm이나 확인 시간 초과는 오류입니다. 발행 호출은 동시에 받을 수 있으며 한 native 세션에서는 순서대로 처리합니다.
- 전송·확인 오류로 세션이 불확실해지면 해당 자원에서 새 작업을 막습니다. 새 Publisher를 생성한 뒤 재시도해야 합니다. 자동 재발행은 수행하지 않습니다. 오류나 취소 전에 브로커가 수신했을 수 있어 중복 방지는 애플리케이션 책임입니다.
- native 송신 메서드는 context를 무시하므로 플랫폼이 취소·시간 초과 시 소켓을 닫아 대기를 해제합니다. 이 경우 확인 결과가 불확실할 수 있습니다.
- Close는 새 작업을 막고 진행 중인 전달 확인·발행이 끝난 뒤 연결을 한 번 정리합니다. context는 완료 대기만 제한하고 후속 Close로 다시 기다릴 수 있습니다. 정상 정리의 native 연결 종료에는 최대 5초를 사용합니다.
- reject된 메시지의 보존은 실제 DLX 토폴로지에 달려 있습니다. DLX가 없으면 버려질 수 있습니다. Requeue는 영구 실패 메시지를 반복 전달할 수 있습니다.

## 검증

```sh
go test -race ./... -count=1
bash scripts/check-module-boundary.sh
SPINE_TEST_RABBITMQ_URL=amqp://guest:guest@localhost:5672/ go test -race . -run '^TestLive' -count=1 -timeout=2m
```

브로커 환경변수가 없으면 live 테스트는 건너뜁니다. Live 테스트는 binary 본문·header 왕복, mandatory return, publisher confirm, requeue 재전달과 reject 후 dead-letter 보존을 검사합니다. CI는 단위·race 검사와 RabbitMQ 기반 live 검사를 분리해서 실행합니다. 운영 TLS·인증·클러스터 장애는 별도로 검증해야 합니다.

라이선스: [MIT](LICENSE). 계약: [Spine adapter/event](https://github.com/NARUBROWN/spine/tree/1889d66/adapter/event).
