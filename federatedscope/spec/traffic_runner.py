"""Bind the gateway to the existing standalone message handlers."""

import json
import pickle
import random
from collections import deque
from copy import copy
from pathlib import Path

from federatedscope.spec.traffic import Gateway


MODEL_MESSAGES = {'model_para', 'spec_model_para'}


class TrafficOutbox(deque):
    def append(self, message):
        message = copy(message)
        if (message.msg_type in MODEL_MESSAGES
                or message.sender == 0 and message.msg_type == 'evaluate'):
            message.content = pickle.dumps(message.content,
                                           protocol=pickle.HIGHEST_PROTOCOL)
        super().append(message)


class TrafficRun:
    def __init__(self, cfg, handle_message):
        options = cfg.traffic
        clients = range(1, cfg.federate.client_num + 1)
        slow = random.Random(cfg.seed).sample(clients, len(clients) // 5) \
            if options.profile == 'X' else []
        drop = (options.drop_start_s, options.drop_end_s) \
            if options.profile == 'D' else None
        self.gateway = Gateway(
            policy=options.policy, shared_mbps=options.shared_mbps,
            chunk_bytes=options.chunk_bytes,
            access_mbps=dict.fromkeys(slow, 5), drop=drop, rho=options.rho,
            rho_min=options.rho_min, rho_max=options.rho_max,
            interval=options.interval_s, low=options.low_s, high=options.high_s,
            increase=options.increase, decrease=options.decrease)
        self.handle_message = handle_message
        self.rounds = {}
        self.evals = []
        self.spec_jobs = 0
        self.applied = set()
        self.target = options.target_val_acc
        self.seed = cfg.seed
        self.method = cfg.federate.method
        self.profile = options.profile

    def run(self, outbox):
        self.outbox = outbox
        self._drain()
        self.gateway.run()

    def _drain(self):
        while self.outbox:
            message = self.outbox.popleft()
            receivers = message.receiver
            if not isinstance(receivers, list):
                receivers = [receivers]
            ready = max(self.gateway.now, message.timestamp or 0.0)
            for receiver in dict.fromkeys(receivers):
                if receiver == 0 and message.sender != 0 \
                        and message.msg_type in MODEL_MESSAGES:
                    kind = 'spec' if message.msg_type == 'spec_model_para' else 'normal'
                    self.gateway.upload(
                        int(message.sender), message.state, kind,
                        len(message.content), ready,
                        lambda time, msg=message: self._deliver(msg, 0, time, True))
                elif message.sender == 0 and receiver != 0 and (
                        message.msg_type in MODEL_MESSAGES or message.msg_type == 'evaluate'):
                    self.gateway.call_at(ready, self._download, message, receiver)
                else:
                    self.gateway.call_at(ready, self._deliver, message, receiver, ready)

    def _download(self, message, receiver):
        self.gateway.download(
            int(receiver), message.state, len(message.content),
            lambda time: self._deliver(message, receiver, time, True))

    def _deliver(self, original, receiver, time, serialized=False):
        message = copy(original)
        message.timestamp = time
        message.receiver = [receiver]
        if serialized:
            message.content = pickle.loads(original.content)
        self.handle_message(message)
        if receiver != 0 and message.sender == 0 and message.msg_type == 'spec_model_para':
            self.spec_jobs += 1
        self._drain()

    def observe_round(self, state, normal_ids, normal_required,
                      spec_ids=(), spec_required=None):
        record = self.rounds.setdefault(state, {
            'round': state, 'normal_required': normal_required,
            'spec_required': spec_required, 'normal_ready_s': None,
            'spec_ready_s': None, 'aggregate_s': None,
        })
        if len(normal_ids) >= normal_required and record['normal_ready_s'] is None:
            record['normal_ready_s'] = self.gateway.now
        if (spec_required is not None and len(spec_ids) >= spec_required
                and record['spec_ready_s'] is None):
            record['spec_ready_s'] = self.gateway.now

    def aggregated(self, state, normal_ids, applied_spec_ids=()):
        record = self.rounds[state]
        record['aggregate_s'] = self.gateway.now
        record['normal_clients'] = sorted(map(int, normal_ids))
        record['applied_spec_clients'] = sorted(map(int, applied_spec_ids))
        self.applied.update((state, client) for client in record['applied_spec_clients'])

    @property
    def completed_rounds(self):
        return sum(row['aggregate_s'] is not None for row in self.rounds.values())

    def evaluate(self, state, metrics):
        def accuracy(split):
            value = metrics.get(f'{split}_acc')
            return None if value is None else float(value)

        self.evals.append({
            'completed_rounds': state, 'time_s': self.gateway.now,
            'val_acc': accuracy('val'), 'test_acc': accuracy('test'),
        })

    def save(self, outdir, elapsed, budget):
        report = self.gateway.report()
        hit = next((item for item in self.evals if item['val_acc'] is not None
                    and item['val_acc'] >= self.target), None)
        final = next((item for item in reversed(self.evals)
                      if item['completed_rounds'] == budget), {})
        has_val = any(item['val_acc'] is not None for item in self.evals)
        report.update({
            'seed': self.seed, 'method': self.method, 'profile': self.profile,
            'policy': self.gateway.policy, 'shared_mbps': self.gateway.shared_rate / 1e6,
            'slow_clients': sorted(self.gateway.access_mbps),
            'round_budget': budget, 'completed_rounds': self.completed_rounds,
            'rounds': list(self.rounds.values()), 'evaluations': self.evals,
            'spec_jobs_executed': self.spec_jobs,
            'spec_unique_updates_applied': len(self.applied),
            'final_test_acc': final.get('test_acc'),
            'target_val_acc': self.target,
            'tta_s': hit['time_s'] if hit else None,
            'tta_status': 'reached' if hit else ('not_reached' if has_val else 'no_validation'),
            'simulation_s': self.gateway.now, 'runner_wall_s': elapsed,
        })
        path = Path(outdir) / 'traffic_metrics.json'
        path.write_text(json.dumps(report, indent=2), encoding='utf-8')
