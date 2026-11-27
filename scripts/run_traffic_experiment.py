"""Run paired experiments based on the official standalone FEMNIST YAML."""

import argparse
import json
import subprocess
import sys
from pathlib import Path
from time import perf_counter

from traffic_dependencies import use_workspace_dependencies


ROOT = Path(__file__).resolve().parents[1]
MODES = ('fifo', 'strict', 'fixed', 'adaptive', 'fedavg')


def toy_femnist(root):
    """Write a small, deterministic LEAF fixture for the standalone test."""
    import torch

    generator = torch.Generator().manual_seed(1234)
    processed = root / 'femnist' / 'processed'
    for client in range(12):
        folder = processed / f'task_{client}'
        folder.mkdir(parents=True, exist_ok=True)
        for split, count in [('train', 20), ('val', 5), ('test', 5)]:
            images = torch.randint(0, 256, (count, 784), generator=generator,
                                   dtype=torch.uint8)
            labels = torch.arange(count) % 62
            torch.save((images, labels), folder / f'{split}.pt')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('stage', choices=('pilot', 'compare', 'full', 'smoke'))
    parser.add_argument('--rounds', type=int)
    parser.add_argument('--seeds', type=int, nargs='+')
    parser.add_argument('--profiles', choices=('H', 'X', 'D'), nargs='+')
    parser.add_argument('--mbps', choices=(10, 30, 100), type=int, nargs='+')
    parser.add_argument('--target', type=float, default=0.70)
    parser.add_argument('--drop-start', type=float, default=15.0)
    parser.add_argument('--drop-end', type=float, default=45.0)
    parser.add_argument('--cpu', action='store_true')
    args = parser.parse_args()
    use_workspace_dependencies()
    full = args.stage == 'full'
    smoke = args.stage == 'smoke'
    rounds = args.rounds if args.rounds is not None else (500 if full else 10)
    if rounds < 9:
        parser.error('At least 9 rounds are needed for speculative aggregation')
    if not 0 <= args.drop_start < args.drop_end:
        parser.error('Expected 0 <= drop-start < drop-end')
    modes = ('adaptive',) if args.stage in ('pilot', 'smoke') else MODES
    seeds = args.seeds or ([12345, 12346, 12347] if full else [12345])
    profiles = args.profiles or (['H', 'X', 'D'] if full else ['H'])
    rates = args.mbps or ([10, 30, 100] if full else [30])
    data_root = 'data/traffic_synthetic' if smoke else 'data/traffic_femnist'
    if smoke:
        toy_femnist(ROOT / data_root)

    for seed in seeds:
        for profile in profiles:
            for rate in rates:
                for mode in modes:
                    label = f'{mode}_{profile}_{rate}mbps_s{seed}_r{rounds}'
                    outdir = f'exp/traffic/{args.stage}'
                    options = {
                        'outdir': outdir, 'expname': label, 'seed': seed,
                        'use_gpu': not (args.cpu or smoke),
                        'federate.total_round_num': rounds,
                        'federate.merge_val_data': True,
                        'data.root': data_root, 'data.splits': [0.7, 0.1, 0.2],
                        'eval.split': ['val', 'test'],
                        'eval.freq': 1 if rounds <= 10 else 10,
                        'traffic.enabled': True, 'traffic.profile': profile,
                        'traffic.policy': 'fifo' if mode == 'fedavg' else mode,
                        'traffic.shared_mbps': rate, 'traffic.target_val_acc': args.target,
                        'traffic.drop_start_s': args.drop_start,
                        'traffic.drop_end_s': args.drop_end,
                        'spec.sample_client_num': 3,
                    }
                    if mode == 'fedavg':
                        options.update({'federate.method': 'fedavg', 'spec.use': False,
                                        'spec.start_spec_round': 6})
                    if smoke:
                        options.update({'federate.client_num': 12,
                                        'federate.sample_client_num': 4,
                                        'spec.predictor.epochs': 1})
                    command = [sys.executable, '-m', 'federatedscope.main', '--cfg',
                               'scripts/example/swiftfl_convnet2_on_femnist.yaml']
                    for key, value in options.items():
                        command.extend((key, value if isinstance(value, str) else repr(value)))
                    expected = ROOT / outdir / 'femnist' / label
                    before = set(expected.glob('**/traffic_metrics.json'))
                    print(f'Running {label}', flush=True)
                    start = perf_counter()
                    subprocess.run(command, cwd=ROOT, check=True)
                    wall = perf_counter() - start
                    paths = set(expected.glob('**/traffic_metrics.json')) - before
                    if len(paths) != 1:
                        raise RuntimeError(f'Expected one new result under {expected}')
                    path = paths.pop()
                    report = json.loads(path.read_text(encoding='utf-8'))
                    report['experiment_wall_s'] = wall
                    path.write_text(json.dumps(report, indent=2), encoding='utf-8')
                    verify(report, rounds, mode)
                    print(path, flush=True)


def verify(report, rounds, mode):
    if report['completed_rounds'] != rounds or report['final_test_acc'] is None \
            or report['tta_status'] == 'no_validation':
        raise RuntimeError('Missing completed rounds or evaluation')
    received = report['updates']
    ids = [item['payload_id'] for item in received]
    normal = [item for item in received if item['kind'] == 'normal']
    spec = [item for item in received if item['kind'] == 'spec']
    if len(ids) != len(set(ids)) or not normal or not report['bytes']['download']:
        raise RuntimeError('Unexpected network delivery trace')
    if report['bytes']['access'] != report['bytes']['normal'] + report['bytes']['spec']:
        raise RuntimeError('The access and shared links transferred different byte counts')
    for row in report['rounds']:
        ready = [row['normal_ready_s']]
        if row['spec_required'] is not None:
            ready.append(row['spec_ready_s'])
        if None in ready or row['aggregate_s'] != max(ready):
            raise RuntimeError(f'Invalid aggregation timing: {row}')
    if mode == 'fedavg':
        if spec or report['spec_jobs_executed'] or report['spec_unique_updates_applied']:
            raise RuntimeError('FedAvg emitted speculative work')
    elif not (spec and report['spec_jobs_executed'] and report['spec_unique_updates_applied']):
        raise RuntimeError('SwiftFL did not complete and incorporate speculative work')


if __name__ == '__main__':
    main()
