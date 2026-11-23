# SwiftFL 혼잡 인지 트래픽 스케줄링 설계

## 기존 실행 경로와 집계

공식 `scripts/example/swiftfl_convnet2_on_femnist.yaml`은 단일 프로세스 standalone에서 `SpecStandaloneRunner`를 선택한다. worker registry는 `IdpSpec`을 `SpecClient`와 `idpSpecServer`에 연결한다. normal 모델은 core Client의 `callback_funcs_for_model_para`, speculative 모델은 SpecClient의 `callback_funcs_for_spec_model_para`에서 로컬 학습을 시작한다. 두 경로는 받은 모델 메시지와 같은 종류의 이름으로 서버에 업데이트를 돌려준다. normal은 `baseServer.callback_funcs_model_para`에서 타임스탬프를 갱신한 뒤 core Server 핸들러로 넘어가고, speculative는 `idpSpecServer.callback_funcs_spec_model_para`가 현재 버전과 일치하는 결과를 버퍼에 넣는다.

기존 standalone 루프는 공용 deque에서 클라이언트 방향 메시지를 직접 전달하고 서버 방향 메시지를 timestamp 순으로 꺼낸다. 확장 옵션이 켜지면 이 루프에 도착 이벤트를 연결한다. 전송 시점에 payload를 복제하고, 송수신 방향과 메시지 종류로 학습 트래픽을 식별한다. 모델 다운로드가 끝난 클라이언트만 기존 핸들러를 실행하고, 업로드 전체가 수신될 때 서버 핸들러를 한 번 실행한다. Predictor, Compensator, Coordinator, 모델 버전 검사와 샘플러를 그대로 사용한다.

`check_and_move_on()`은 일반 라운드에서 normal 정원 전체를, speculative 집계 라운드(`state > start_spec_round + 1` 및 예측 주기 일치)에서 normal과 speculative 정원을 모두 확인한다. 공식 warm-up 설정의 `start_spec_round`는 6이고 첫 speculative 집계 후보는 0 기반 라운드 8이다. `optimal_all`의 공식 `spec.sample_client_rate=0.1`은 100명 중 10명이다. 실험에서는 공식 `spec.sample_client_num=3`을 지정한다. 대역폭 비율 ρ와 참여자 수는 독립적이다.

## 링크와 시간 모델

클라이언트별 access uplink에서 직렬화가 끝난 chunk가 공용 게이트웨이에 들어간다. 공용 업링크는 10/30/100 Mbps이며 한 번에 chunk 하나를 직렬화한다. 기본 chunk는 16 KiB이며 마지막 chunk는 실제 바이트를 사용한다. 송신별 payload ID, 클라이언트 ID, 버전, chunk 번호와 크기를 추적한다. 모델 다운로드는 수신자별 unicast로 별도의 50 Mbps FIFO 링크에 넣는다. 제어 메시지는 지연 없이 전달한다. 전파 지연과 헤더 비용은 0이며 바이트는 직렬화된 payload 크기로 정의한다.

H의 access는 모두 100 Mbps, X는 별도 seed 난수로 선택한 20%의 access가 5 Mbps다. D는 H에 더해 지정된 시뮬레이션 구간 동안 공용 uplink가 1/3 속도로 작동한다. 진행 중 chunk는 구간별 속도로 남은 비트만 전송한다.

FIFO는 공용 큐 도착 순, strict는 chunk 경계에서 normal 우선이다. Fixed와 adaptive는 두 큐에 backlog가 함께 있을 때 실제 서비스 바이트와 ρ로 가중 서비스 편차를 갱신한다. 단일 큐는 전체 링크를 쓰며 경쟁 구간의 편차는 초기화한다. Adaptive는 100 ms마다 전송 중인 chunk를 제외한 normal 선두 대기시간을 본다. 300 ms 초과 시 ρ를 절반으로 내리고 카운터를 0으로 만든다. 100 ms 미만 두 번 연속이면 0.05를 더하고 카운터를 0으로 만든다. 빈 큐와 중간 범위(경계 포함)도 카운터를 0으로 만든다. ρ는 [0.05, 0.80] 범위이며 초기값은 0.30이다.

클라이언트 연산 준비 시각은 공식 장치 기반 계산 비용과 클라이언트의 직렬 실행 제약으로 정한다. 기존 통신 추정값은 제외한다. 서버 Predictor 학습·동기화·예측, 보정, 집계와 평가의 실제 연산은 수행하되 시뮬레이션에서는 비용 0으로 두는 네트워크 중심 모델을 사용한다. 이 연산을 포함한 runner 실제 경과시간은 별도로 기록한다.

## 측정과 해석

normal chunk의 게이트웨이 대기 평균과 p95, 종류별 준비→서버 전체 수신 지연, 실제 바이트, ρ 시계열, 라운드별 두 정원 충족과 집계 시각을 저장한다. speculative 다운로드가 클라이언트 학습을 실행한 횟수와 모델 반영 경로가 완료된 고유 (버전, 클라이언트) 집합을 따로 센다. 동일 seed별 분할과 장치 정보로 각 방식을 새로 학습한다. FEMNIST의 검증 분할을 확보하고 서버에서 집계하며 최초 validation 목표 도달 시각과 예산 마지막 평가의 test accuracy를 기록한다.

normal 지연 개선만으로는 집계 시각이 앞당겨지지 않는다. 두 정원이 필요한 라운드는 느린 정원이 병목이다. 슬라이드 5에서 10 Mbps로 각 1,000,000 bytes를 전송하면 normal 완료는 FIFO 1.60초, strict 0.80초지만 두 정원 완료와 집계 가능 시각은 모두 1.60초다. 실제 학습에서 업로드가 준비되는 시각이 서로 다르면 두 정원 중 늦은 결과가 달라질 수 있다. 검증 목표 도달 시간은 평가 시각과 최적화 경로에도 의존한다.
