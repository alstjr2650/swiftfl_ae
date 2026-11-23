"""Event-driven links for the single-process SwiftFL runner."""

import heapq
import itertools
import math
from collections import deque
from dataclasses import dataclass
from statistics import mean


@dataclass
class Payload:
    id: int
    client: int
    version: int
    kind: str
    size: int
    ready: float
    remaining: int
    done: object


@dataclass(frozen=True)
class Chunk:
    payload: Payload
    index: int
    size: int
    queued: float
    order: int


class Gateway:
    def __init__(self, policy='fifo', shared_mbps=30, chunk_bytes=16384,
                 access_mbps=None, down_mbps=50, drop=None, rho=0.30,
                 rho_min=0.05, rho_max=0.80, interval=0.1, low=0.1,
                 high=0.3, increase=0.05, decrease=0.5):
        if policy not in ('fifo', 'strict', 'fixed', 'adaptive'):
            raise ValueError(f'Unknown traffic policy {policy}')
        if min(shared_mbps, chunk_bytes, down_mbps, interval) <= 0:
            raise ValueError('Link speeds, chunk size and tick must be positive')
        if not (0 <= rho_min <= rho <= rho_max <= 1 and 0 <= low < high
                and increase > 0 and 0 < decrease < 1):
            raise ValueError('Invalid share or feedback settings')
        if drop and not (0 <= drop[0] < drop[1]):
            raise ValueError('Invalid capacity drop interval')
        self.now = 0.0
        self.policy = policy
        self.shared_rate = shared_mbps * 1e6
        self.chunk_bytes = chunk_bytes
        self.access_mbps = access_mbps or {}
        self.down_rate = down_mbps * 1e6
        self.drop = drop
        self.rho = rho
        self.rho_min, self.rho_max = rho_min, rho_max
        self.interval, self.low, self.high = interval, low, high
        self.increase, self.decrease = increase, decrease
        self.low_count = 0
        self.balance = 0.0
        self.rho_history = [(0.0, rho)]
        self.events = []
        self.sequence = itertools.count()
        self.tick = 1
        self.payload_ids = itertools.count()
        self.queues = {'normal': deque(), 'spec': deque()}
        self.access_free = {}
        self.down_free = 0.0
        self.busy = False
        self.waits = []
        self.bytes = {'normal': 0, 'spec': 0, 'download': 0, 'access': 0}
        self.updates = []
        self.downloads = 0

    def call_at(self, time, callback, *args):
        if time < self.now:
            raise ValueError('Cannot schedule an event in the past')
        heapq.heappush(self.events, (time, next(self.sequence), callback, args))

    def upload(self, client, version, kind, size, ready, done, at_gateway=False):
        payload = self._payload(client, version, kind, size, ready, done)
        self.call_at(ready, self._start_upload, payload, at_gateway)

    def _payload(self, client, version, kind, size, ready, done):
        if size <= 0:
            raise ValueError('A payload must contain at least one byte')
        count = (size + self.chunk_bytes - 1) // self.chunk_bytes
        return Payload(next(self.payload_ids), client, version, kind, size,
                       ready, count, done)

    def _chunks(self, payload):
        for index, offset in enumerate(range(0, payload.size, self.chunk_bytes)):
            yield index, min(self.chunk_bytes, payload.size - offset)

    def _start_upload(self, payload, at_gateway):
        at = self.now
        if not at_gateway:
            at = max(at, self.access_free.get(payload.client, 0))
        rate = self.access_mbps.get(payload.client, 100) * 1e6
        for index, size in self._chunks(payload):
            if not at_gateway:
                at += size * 8 / rate
            chunk = Chunk(payload, index, size, at, next(self.sequence))
            self.call_at(at, self._enqueue, chunk, at_gateway)
        if not at_gateway:
            self.access_free[payload.client] = at

    def _enqueue(self, chunk, at_gateway):
        if not at_gateway:
            self.bytes['access'] += chunk.size
        self.queues[chunk.payload.kind].append(chunk)

    def download(self, client, version, size, done):
        payload = self._payload(client, version, 'download', size, self.now, done)
        start = max(self.now, self.down_free)
        total = 0
        for index, chunk_size in self._chunks(payload):
            total += chunk_size
            self.down_free = start + total * 8 / self.down_rate
            chunk = Chunk(payload, index, chunk_size, self.now, next(self.sequence))
            self.call_at(self.down_free, self._finish_download, chunk)

    def _finish_download(self, chunk):
        payload = chunk.payload
        self.bytes['download'] += chunk.size
        payload.remaining -= 1
        if payload.remaining == 0:
            self.downloads += 1
            payload.done(self.now)

    def observe(self, delay):
        old = self.rho
        if delay is not None and delay > self.high:
            self.rho = max(self.rho_min, self.decrease * self.rho)
            self.low_count = 0
        elif delay is not None and delay < self.low:
            self.low_count += 1
            if self.low_count == 2:
                self.rho = min(self.rho_max, self.rho + self.increase)
                self.low_count = 0
        else:
            self.low_count = 0
        if self.rho != old:
            self.rho_history.append((self.now, self.rho))

    def _completion(self, bits):
        if self.drop:
            start, end = self.drop
            time = self.now
            for boundary, rate in ((start, self.shared_rate),
                                   (end, self.shared_rate / 3)):
                if time < boundary:
                    capacity = rate * (boundary - time)
                    if bits <= capacity:
                        return time + bits / rate
                    bits -= capacity
                    time = boundary
            return time + bits / self.shared_rate
        return self.now + bits / self.shared_rate

    def _dispatch(self):
        normal, spec = self.queues.values()
        if self.busy or not (normal or spec):
            return
        if not (normal and spec):
            queue = normal or spec
            self.balance = 0.0
        elif self.policy == 'fifo':
            normal_key = (normal[0].queued, normal[0].order)
            spec_key = (spec[0].queued, spec[0].order)
            queue = normal if normal_key < spec_key else spec
        elif self.policy == 'strict':
            queue = normal
        else:
            queue = spec if self.balance >= 0 else normal
            size = queue[0].size
            self.balance += self.rho * size - (size if queue is spec else 0)
        chunk = queue.popleft()
        if chunk.payload.kind == 'normal':
            self.waits.append(self.now - chunk.queued)
        self.busy = True
        self.call_at(self._completion(chunk.size * 8), self._finish_chunk, chunk)

    def _finish_chunk(self, chunk):
        self.busy = False
        payload = chunk.payload
        self.bytes[payload.kind] += chunk.size
        payload.remaining -= 1
        if payload.remaining == 0:
            self.updates.append({
                'payload_id': payload.id, 'client': payload.client,
                'version': payload.version, 'kind': payload.kind,
                'bytes': payload.size, 'ready_s': payload.ready,
                'received_s': self.now, 'latency_s': self.now - payload.ready,
            })
            payload.done(self.now)

    def run(self):
        while self.events:
            arrival = self.events[0][0]
            sample = self.tick * self.interval if self.policy == 'adaptive' else math.inf
            self.now = min(arrival, sample)
            while self.events and self.events[0][0] <= self.now:
                _, _, callback, args = heapq.heappop(self.events)
                callback(*args)
            if sample <= self.now:
                normal = self.queues['normal']
                self.observe(self.now - normal[0].queued if normal else None)
                self.tick += 1
            self._dispatch()

    def report(self):
        waits = sorted(self.waits)
        latencies = {}
        for kind in ('normal', 'spec'):
            values = sorted(item['latency_s'] for item in self.updates
                            if item['kind'] == kind)
            latencies[kind] = {'count': len(values),
                               'mean_s': mean(values) if values else None,
                               'p95_s': values[math.ceil(.95 * len(values)) - 1]
                               if values else None}
        return {
            'normal_chunk_queue_mean_s': mean(waits) if waits else None,
            'normal_chunk_queue_p95_s': waits[math.ceil(0.95 * len(waits)) - 1]
            if waits else None,
            'bytes': self.bytes, 'updates': self.updates,
            'update_latency': latencies,
            'downloads': self.downloads, 'rho_history': self.rho_history,
        }
