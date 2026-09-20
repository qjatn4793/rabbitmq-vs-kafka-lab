#!/bin/bash
# rabbit2, rabbit3 을 rabbit1 클러스터에 합류시킨다.
# 세 노드가 모두 healthy 해진 다음에 실행해야 한다.
set -euo pipefail

for node in rabbit2 rabbit3; do
  echo "== mqlab-$node → rabbit@rabbit1 합류 =="
  docker exec "mqlab-$node" rabbitmqctl stop_app
  docker exec "mqlab-$node" rabbitmqctl reset
  docker exec "mqlab-$node" rabbitmqctl join_cluster rabbit@rabbit1
  docker exec "mqlab-$node" rabbitmqctl start_app
done

echo
docker exec mqlab-rabbit1 rabbitmqctl cluster_status
