# rabbitmq-vs-kafka-lab

RabbitMQ와 Kafka의 동작 차이를 직접 재현해서 확인하는 실험 코드입니다.
표로 정리된 비교는 많지만 실제로 돌려본 로그는 드물어서, 아래 네 가지를 재현하고 결과를 기록합니다.

## 실험 목록

| # | 실험 | 확인하는 것 |
|---|------|------------|
| 1 | 재처리 replay | Kafka는 오프셋을 되감아 재소비할 수 있고, RabbitMQ 큐는 ack 시 메시지가 사라진다 |
| 2 | 순서 역전 | 하나의 큐에 여러 컨슈머가 붙었을 때 처리 순서가 깨지는 과정 |
| 3 | 파티션 키 | Kafka에서 키 없이 보내면 순서 보장이 무의미해지는 이유 |
| 4 | head-of-line blocking | 실패 한 건이 Kafka 파티션 전체를 멈추는 반면 RabbitMQ는 DLQ로 격리된다 |

## 작성 예정

- 실행 방법: docker compose 로 RabbitMQ와 Kafka 동시 기동
- - 실험별 재현 절차와 실제 출력 로그
  - - 환경 정보: RabbitMQ / Kafka 버전, 파티션 및 컨슈머 수, prefetch 설정
    - - 관련 블로그 글 링크
      - 
