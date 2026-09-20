#!/bin/bash
# 실험 7 — 발행 중에 리더 노드를 강제로 죽였다가 되살린다.
#   ./cluster/failover.sh kafka
#   ./cluster/failover.sh rabbit
set -euo pipefail

BROKER=${1:-kafka}        # kafka | rabbit
DURATION=${2:-45}         # 전체 발행 시간(초)
KILL_AT=${3:-10}          # 몇 초 뒤에 죽일지
DOWN_FOR=${4:-15}         # 몇 초 동안 죽여둘지

start=$(curl -sf "http://localhost:8080/experiments/7/${BROKER}/start?durationSec=${DURATION}")
container=$(echo "$start" | python3 -c "import sys,json; print(json.load(sys.stdin)['leaderContainer'])")
echo ">> 리더: $container  (전체 ${DURATION}초 발행)"

sleep "$KILL_AT"
echo ">> [$(date +%T)] docker stop $container"
docker stop "$container" >/dev/null

sleep "$DOWN_FOR"
echo ">> [$(date +%T)] docker start $container"
docker start "$container" >/dev/null

echo ">> 발행 종료 대기..."
curl -sf --max-time 300 "http://localhost:8080/experiments/7/${BROKER}/result"
