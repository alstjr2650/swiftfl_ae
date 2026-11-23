import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from federatedscope.spec.traffic import Gateway
from federatedscope.spec.traffic_runner import TrafficOutbox, TrafficRun


def traffic_config():
    return SimpleNamespace(
        traffic=SimpleNamespace(policy='strict', profile='H', shared_mbps=10,
                                chunk_bytes=2, rho=0.3, rho_min=0.05,
                                rho_max=0.8, interval_s=0.1, low_s=0.1,
                                high_s=0.3, increase=0.05, decrease=0.5,
                                target_val_acc=0.7),
        federate=SimpleNamespace(client_num=2, method='IdpSpec'), seed=1)


class GatewayTest(unittest.TestCase):
    def test_slide_5(self):
        for policy, normal in [('fifo', 1.60), ('strict', 0.80)]:
            with self.subTest(policy=policy):
                received = {}
                gateway = Gateway(policy=policy, shared_mbps=10)
                for kind in ('spec', 'normal'):
                    gateway.upload(1, 8, kind, 1_000_000, 0,
                                   lambda t, k=kind: received.update({k: t}),
                                   at_gateway=True)
                gateway.run()
                self.assertAlmostEqual(received['normal'], normal)
                self.assertAlmostEqual(received['spec'], 2.40 - normal)
                self.assertAlmostEqual(max(received.values()), 1.60)
                self.assertEqual(len(received), 2)

    def test_single_queue_is_work_conserving(self):
        for policy in ('fifo', 'strict', 'fixed', 'adaptive'):
            for kind in ('normal', 'spec'):
                received = []
                gateway = Gateway(policy=policy, shared_mbps=10)
                gateway.upload(7, 8, kind, 1_000_001, 0, received.append,
                               at_gateway=True)
                gateway.run()
                self.assertEqual(len(received), 1)
                self.assertAlmostEqual(received[0], 0.8000008)
                self.assertEqual(gateway.bytes[kind], 1_000_001)

    def test_weighted_ratio_uses_bytes_with_partial_chunks(self):
        for policy in ('fixed', 'adaptive'):
            gateway = Gateway(policy=policy, shared_mbps=10, interval=100,
                              chunk_bytes=1000)
            served = []
            for index in range(500):
                for kind, size in [('normal', 1900), ('spec', 1123)]:
                    gateway.upload(index, 8, kind, size, 0, lambda t: None,
                                   at_gateway=True)
            gateway.call_at(0.45, lambda: served.append(gateway.bytes.copy()))
            gateway.run()
            total = served[0]['normal'] + served[0]['spec']
            self.assertLessEqual(abs(served[0]['spec'] - 0.30 * total), 1000)

    def test_feedback_thresholds_counter_reset_and_clamps(self):
        gateway = Gateway(policy='adaptive')
        for reset in (None, 0.1, 0.3, 0.2):
            gateway.observe(0.01)
            self.assertEqual(gateway.low_count, 1)
            gateway.observe(reset)
            self.assertEqual(gateway.low_count, 0)
        self.assertEqual(gateway.rho, 0.30)
        gateway.observe(0.01)
        self.assertEqual(gateway.rho, 0.30)
        gateway.observe(0.01)
        self.assertEqual(gateway.rho, 0.35)
        for _ in range(20):
            gateway.observe(0.01)
        self.assertEqual(gateway.rho, 0.80)
        gateway.observe(0.01)
        gateway.observe(0.301)
        self.assertEqual(gateway.low_count, 0)
        self.assertEqual(gateway.rho, 0.40)
        for _ in range(10):
            gateway.observe(1)
        self.assertEqual(gateway.rho, 0.05)

    def test_capacity_change_inside_chunk(self):
        gateway = Gateway(shared_mbps=10, chunk_bytes=1_000_000, drop=(0.2, 0.5))
        received = []
        gateway.upload(1, 4, 'normal', 1_000_000, 0, received.append,
                       at_gateway=True)
        gateway.run()
        self.assertEqual(len(received), 1)
        self.assertAlmostEqual(received[0], 1.0)
        self.assertEqual(gateway.bytes['normal'], 1_000_000)

    def test_adaptive_clock_excludes_in_service_chunk(self):
        class SamplingGateway(Gateway):
            observations = []

            def observe(self, delay):
                self.observations.append((self.now, delay))
                super().observe(delay)

        gateway = SamplingGateway(policy='adaptive', shared_mbps=10,
                                  chunk_bytes=1_000_000)
        gateway.upload(1, 0, 'normal', 1_000_000, 0, lambda t: None,
                       at_gateway=True)
        gateway.run()
        self.assertEqual([round(t, 1) for t, _ in gateway.observations],
                         [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8])
        self.assertTrue(all(delay is None for _, delay in gateway.observations))
        self.assertEqual(gateway.rho_history, [(0.0, 0.30)])

    def test_fifo_uses_gateway_arrival_not_access_enqueue_order(self):
        gateway = Gateway(shared_mbps=10, access_mbps={1: 5}, chunk_bytes=1000)
        received = []
        gateway.upload(1, 4, 'spec', 1000, 0, lambda t: received.append('spec'))
        gateway.upload(2, 4, 'normal', 1000, 0, lambda t: received.append('normal'))
        gateway.run()
        self.assertEqual(received, ['normal', 'spec'])

    def test_snapshot_download_fifo_and_complete_delivery(self):
        queue = TrafficOutbox()
        received = []
        original = SimpleNamespace(sender=0, receiver=[1, 2], state=8,
                                   msg_type='spec_model_para', content=[5],
                                   timestamp=0)
        queue.append(original)
        down_bytes = len(queue[0].content)
        original.content[0] = 99

        def handle(msg):
            received.append((msg.sender, msg.receiver[0], msg.timestamp,
                             msg.content[0]))
            if msg.sender == 0:
                msg.content[0] += 1
                queue.append(SimpleNamespace(sender=msg.receiver[0], receiver=[0],
                                             state=8, msg_type=msg.msg_type,
                                             content=msg.content,
                                             timestamp=msg.timestamp + 0.01))
                msg.content[0] = 100

        transport = TrafficRun(traffic_config(), handle)
        transport.run(queue)
        self.assertEqual([(a, b, value) for a, b, _, value in received],
                         [(0, 1, 5), (0, 2, 5), (1, 0, 6), (2, 0, 6)])
        for index in range(2):
            self.assertAlmostEqual(received[index][2],
                                   (index + 1) * down_bytes * 8 / 50e6)
            self.assertGreater(received[index + 2][2], received[index][2] + 0.01)

    def test_tta_uses_first_validation_hit_and_final_test_accuracy(self):
        transport = TrafficRun(traffic_config(), lambda msg: None)
        for state, metrics in enumerate([
                {'test_acc': 0.99, 'val_acc': 0.69},
                {'test_acc': 0.60, 'val_acc': 0.70},
                {'test_acc': 0.80, 'val_acc': 0.75}], start=1):
            transport.gateway.call_at(float(state), transport.evaluate, state, metrics)
        transport.run(TrafficOutbox())
        with TemporaryDirectory() as directory:
            transport.save(directory, 12.0, 3)
            path = Path(directory) / 'traffic_metrics.json'
            report = json.loads(path.read_text(encoding='utf-8'))
        self.assertEqual(report['tta_s'], 2.0)
        self.assertEqual(report['tta_status'], 'reached')
        self.assertEqual(report['final_test_acc'], 0.80)
