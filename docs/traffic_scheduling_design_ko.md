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

공식 `make_global_eval` 경로는 normal 평가 모델을 사용한다. speculative 반영 수는 `spec_model`에 집계되고 결합된 업데이트를 센다. 이 모델은 이후 speculative 작업에 쓰인다. 따라서 지연 개선을 확인하더라도 평가 모델이나 TTA의 개선을 별도로 검증해야 한다. ρ 시계열은 초기값과 변화 시각을 저장한다. p95는 nearest-rank 방식이며, access 바이트와 공용 uplink 바이트는 링크별로 기록한다. `runner_wall_s`는 standalone 루프와 Predictor 종료 정리를 포함하고, `experiment_wall_s`는 데이터 로딩과 초기화부터 학습 프로세스가 종료될 때까지 잰다.

## 실험 재현

`scripts/run_traffic_experiment.py`는 공식 예제 YAML을 기반으로 `traffic.enabled=True`를 지정한다. 기본값인 `False`에서는 기존 standalone 전달 루프를 사용한다. FEMNIST의 train/validation/test 비율을 70/10/20으로 조정하고 기존 캐시와 섞이지 않도록 `data/traffic_femnist`를 사용한다. 데이터 전처리 seed는 1234로 고정한다. 실험 seed는 장치 표본, 클라이언트 선택, 학습 및 별도 Predictor 프로세스에 적용한다. 공식 Predictor의 비동기 요청과 동기화 동작을 유지하므로 운영체제와 PyTorch 환경에 따라 학습 반복 횟수나 수치에 차이가 날 수 있다. FedAvg는 같은 normal 선택 규칙과 장치 특성을 적용하고 speculative 학습을 시작하지 않는다. 각 결과의 `rounds[].normal_clients`로 실제 sampling을 검증할 수 있다.

PyTorch와 저장소 의존성이 있는 PowerShell에서 실행한다. CPU 환경에서는 아래처럼 `--cpu`를 전달한다.

```powershell
python -m unittest discover -s tests -v
python scripts/run_traffic_experiment.py smoke --cpu
python scripts/run_traffic_experiment.py pilot --cpu
python scripts/run_traffic_experiment.py compare --cpu
python scripts/run_traffic_experiment.py full --cpu
```

`smoke`는 합성 LEAF 입력에서 실제 ConvNet2와 공식 SwiftFL worker를 10라운드 실행한다. `pilot`은 실제 FEMNIST에서 adaptive 한 방식을, `compare`는 5개 방식 모두를 새로 학습한다. 두 명령의 기본 조건은 H, 30 Mbps, seed 12345, 10라운드, validation 목표 70%다. `full`은 500라운드 예산에서 seed 12345/12346/12347, H/X/D, 10/30/100 Mbps, 5개 방식을 실행한다. D의 기본 용량 감소 구간은 시뮬레이션 15~45초이며 `--drop-start`와 `--drop-end`로 변경할 수 있다. 결과는 `exp/traffic/<단계>/femnist/<실험명>/traffic_metrics.json`에 기록된다.

## 확인한 결과

네트워크 및 측정 단위 테스트 9개를 통과했다. 실제 standalone smoke는 10라운드를 완료하고 speculative 업데이트 6개를 반영했다. 실제 FEMNIST adaptive pilot도 speculative 업데이트 6개를 반영했다. 시뮬레이션 시간은 845.58초, CPU에서 데이터 로딩을 포함한 실행시간은 72.32초였다.

실제 FEMNIST 다섯 방식 비교에서 10라운드 모두의 normal 클라이언트 ID가 일치했다. 아래 결과는 H·30 Mbps·seed 12345이며, 전체 지표는 `exp/traffic/compare/summary_H_30mbps_s12345_r10.csv`와 각 실행의 JSON에 있다.

| 방식 | normal chunk 대기 평균 / p95 (초) | 최종 집계 (시뮬레이션 초) | 최종 test accuracy |
| --- | ---: | ---: | ---: |
| SwiftFL FIFO | 13.87 / 28.24 | 845.58 | 18.55% |
| SwiftFL strict priority | 13.87 / 28.24 | 845.58 | 18.55% |
| SwiftFL fixed share | 14.38 / 32.17 | 845.58 | 18.55% |
| SwiftFL adaptive share | 13.94 / 28.77 | 845.58 | 18.55% |
| FedAvg | 13.87 / 28.24 | 803.31 | 14.72% |

Adaptive의 normal 대기 p95는 fixed보다 낮았고 FIFO보다 높았다. 네 SwiftFL 방식 모두 0 기반 라운드 8과 9에서 마지막 speculative 결과를 받은 직후 집계했다. 다섯 방식 모두 validation 70% 목표에 미도달했다. 3개 seed의 전체 행렬은 위 실행 명령으로 재현할 수 있다.
