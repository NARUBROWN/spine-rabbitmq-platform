#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# 플랫폼 구현은 공개 이벤트 계약만 참조하며 커널과 내부 패키지를 사용하지 않습니다.
unexpected=$(go list -deps -f '{{.ImportPath}}' ./... | awk '
  ($0 == "github.com/NARUBROWN/spine" || index($0, "github.com/NARUBROWN/spine/") == 1) &&
    $0 != "github.com/NARUBROWN/spine/adapter/event"
')
if [ -n "$unexpected" ]; then
  printf 'Platform depends on the kernel implementation:\n%s\n' "$unexpected" >&2
  exit 1
fi
