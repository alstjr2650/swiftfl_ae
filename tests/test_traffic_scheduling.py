import unittest

from federatedscope.spec.traffic import Gateway


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
                self.assertAlmostEqual(max(received.values()), 1.60)
                self.assertEqual(len(received), 2)
