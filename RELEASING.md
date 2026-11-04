# spine-rabbitmq-platform 릴리스 운영

이 작업 소스의 대상 버전은 v0.2.0이며 Spine v0.7.0 계약을 기준으로 검증합니다. 기존 정식 v0.1.0 태그는 변경하지 않습니다.

## 채널과 의존성

| 채널 | 코어 고정 버전 | 이 모듈의 태그 |
| --- | --- | --- |
| Preview | v0.7.0-preview.1 | v0.2.0-preview.1 |
| RC | v0.7.0-rc.1 | v0.2.0-rc.1 |
| 정식 | v0.7.0 | v0.2.0 |

정확한 계획은 `.github/release-policy.json`에 고정합니다. 패키지의 기본 버전은 코어와 독립적이며 같은 채널의 코어를 사용합니다. 다음 후보의 번호는 이미 발행한 번호보다 커야 합니다.

소스 CI는 아직 없는 코어 태그 때문에 중단되지 않도록 `ci_core`의 전체 SHA를 임시 workspace로 연결합니다. 저장소의 go.mod에 replace를 추가하지 않습니다. 발행 CI와 공개 검증은 `GOWORK=off`로 실제 코어 태그를 사용하며 소스 CI를 공개 설치 증거로 대신하지 않습니다.

## 최초 설정

```sh
python3 scripts/release/configure_branches.py --bootstrap
```

이 명령은 preview와 rc 생성 계획만 출력합니다. 검토한 경우에만 `--bootstrap --apply`로 누락된 브랜치를 main에서 만듭니다. 기존 브랜치와 태그는 이동하지 않습니다. 첫 작업 PR을 preview에 반영한 다음 보호 규칙 조회와 `--apply`를 진행합니다.

보호 규칙은 기본 브랜치 preview, 대상별 필수 CI, 승격 방향, RC와 main의 merge commit, 태그 수정과 삭제 금지, 발행 환경을 설정합니다. 정식은 지정 리뷰어 승인과 main 제한, 관리자 우회 금지를 요구합니다. 이 설정의 적용과 PR 게시, 병합, 발행은 사용자가 검토한 후 진행합니다.

## Preview

코어의 해당 Preview 후보 태그를 먼저 발행합니다. 다음 명령으로 이 모듈의 코어 의존성과 실제 공개 체크섬을 준비합니다.

```sh
python3 scripts/release/pin_channel.py --channel preview
python3 scripts/release/pin_channel.py --channel preview --apply --tidy
```

변경을 커밋해 preview에 반영하고 전체 소스 CI를 확인합니다. **Actions → Publish Preview**에서 preview 브랜치, 이 모듈의 새 태그와 HEAD 전체 SHA를 지정합니다.

발행 workflow는 실제 코어 태그로 vet, race와 모듈 재현성을 검사합니다. Kafka와 RabbitMQ 구현은 실제 브로커도 검사합니다. CI와 공개 SHA를 사용한 소비자 검증이 성공하면 태그를 생성하고, 새로운 캐시의 별도 소비자에서 해당 태그로 다시 빌드합니다. 발행된 모듈의 테스트도 실행하며 실패나 skip을 허용하지 않습니다.

성공한 Preview와 RC는 `release-manifest-<태그>` artifact를 남기고 GitHub Release를 만들지 않습니다. artifact에는 소스, 실제 버전 조합, 검증 범위와 workflow 실행 증거가 들어갑니다. 정식만 GitHub Release를 만듭니다.

## 승격

Preview 완료 후 72시간, RC 완료 후 24시간을 관찰합니다. 정식 발행 간격은 168시간이며 긴급 관찰 기간 단축 사유는 20자 이상 기록합니다. CI, 공개 검증과 정식 승인은 생략하지 않습니다.

코어의 다음 채널 후보 태그를 먼저 발행한 후 전용 승격 브랜치에서 의존성을 갱신합니다.

```sh
git switch -c promote/rc/v0.2.0-rc.1 v0.2.0-preview.1
python3 scripts/release/pin_channel.py --channel rc --apply --tidy
```

RC 승격 PR은 rc를 대상으로 합니다. 정식은 `promote/stable/v0.2.0`에서 stable 핀을 준비하고 main을 대상으로 합니다. 일반 변경은 preview를 대상으로 합니다. 승격 PR은 merge commit을 사용합니다.

검증된 후보의 후속 커밋인지, 코드 fingerprint가 같은지, 같은 기본 버전의 공식 의존성 채널 변경만 있는지 검사합니다. 코드나 일반 라이브러리, 코어 기본 버전이 바뀌면 새 Preview가 필요합니다. 태그는 재사용하지 않습니다.

다섯 플랫폼을 발행한 후 코어 저장소의 **Complete Core Release Cohort**를 실행해야 코어의 조합도 완료됩니다. 하나의 플랫폼 발행 성공이 전체 조합 완료를 뜻하지 않습니다. 예제는 정식 조합 완료 후 갱신하며 이 모듈 후보의 선행 조건이 아닙니다.

## 검증 명령

```sh
python3 -m unittest discover -s test/release -p 'test_*.py'
node --test scripts/release/branch_flow.test.cjs
python3 scripts/release/release.py validate-policy
python3 scripts/release/release.py check-module --channel preview
actionlint -shellcheck= -pyflakes=
```

태그가 공개되기 전에는 코어 소스와 임시 workspace로 Go 테스트를 실행합니다. 공개 CI와 실제 발행 성공은 원격에서 별도로 확인해야 합니다.
