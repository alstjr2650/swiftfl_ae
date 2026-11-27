"""Plot completed SwiftFL traffic runs for one seed and network condition."""

import argparse
import json
import os
from pathlib import Path

from traffic_dependencies import use_workspace_dependencies

use_workspace_dependencies()

import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt


METHODS = {
    'fifo': 'FIFO', 'strict': 'Strict', 'fixed': 'Fixed',
    'adaptive': 'Adaptive', 'fedavg': 'FedAvg',
}


def load_results(directory, condition):
    results = {}
    for mode in METHODS:
        paths = (directory / f'{mode}_{condition}').rglob('traffic_metrics.json')
        for path in sorted(paths, key=lambda item: item.stat().st_mtime, reverse=True):
            run = json.loads(path.read_text(encoding='utf-8'))
            if (run['completed_rounds'] == run['round_budget']
                    and run['final_test_acc'] is not None):
                results[mode] = run
                break
    if not results:
        raise SystemExit(f'No completed results for {condition} under {directory}')
    return results


def plot(results, title, output):
    fig, axes = plt.subplots(2, 3, figsize=(15, 8), layout='constrained')
    fig.suptitle(title, fontsize=16, fontweight='bold')
    queue, upload, validation, timing, accuracy, share = axes.flat
    positions = list(range(len(results)))
    labels = [METHODS[mode] for mode in results]
    for index, (mode, run) in enumerate(results.items()):
        label = METHODS[mode]
        color = f'C{index}'
        if index == 0:
            legend = ('Mean', 'P95', 'Normal', 'Speculative')
        else:
            legend = (None,) * 4
        queue.bar(index - .17, run['normal_chunk_queue_mean_s'], .34,
                  color='#3478a4', label=legend[0])
        queue.bar(index + .17, run['normal_chunk_queue_p95_s'], .34,
                  color='#7ab6d3', label=legend[1])
        for shift, kind, shade, marker in [
                (-.17, 'normal', '#3478a4', legend[2]),
                (.17, 'spec', '#d77a43', legend[3])]:
            delay = run['update_latency'][kind]['mean_s']
            if delay is not None:
                upload.bar(index + shift, delay, .34, color=shade, label=marker)
        evaluations = [row for row in run['evaluations'] if row['val_acc'] is not None]
        validation.plot([row['time_s'] for row in evaluations],
                        [100 * row['val_acc'] for row in evaluations],
                        '.-', color=color, label=label)
        if run['tta_s'] is not None:
            validation.axvline(run['tta_s'], color=color, alpha=.4)
        timing.plot([row['round'] + 1 for row in run['rounds']],
                    [row['aggregate_s'] for row in run['rounds']],
                    '.-', color=color, label=label)
        accuracy.bar(index, 100 * run['final_test_acc'], color=color)
        if mode in ('fixed', 'adaptive'):
            times, values = map(list, zip(*run['rho_history']))
            share.step(times + [run['simulation_s']], values + [values[-1]],
                       where='post', color=color, label=label)

    for target in sorted({100 * run['target_val_acc'] for run in results.values()}):
        validation.axhline(target, linestyle='--', color='0.5', label=f'Target {target:g}%')
    for axis, heading, ylabel in [
            (queue, 'Normal chunk queue wait', 'Seconds'),
            (upload, 'Mean update delivery latency', 'Seconds from update ready'),
            (validation, 'Validation accuracy over time', 'Accuracy (%)'),
            (timing, 'Round aggregation time', 'Simulated seconds'),
            (accuracy, 'Final test accuracy', 'Accuracy (%)'),
            (share, 'Configured speculative share', 'Speculative share (ρ)')]:
        axis.set_title(heading)
        axis.set_ylabel(ylabel)
        axis.grid(axis='y', alpha=.22)
        axis.set_axisbelow(True)
        if axis.get_legend_handles_labels()[0]:
            axis.legend(fontsize=8)
    for axis in [queue, upload, accuracy]:
        axis.set_xticks(positions, labels)
    for axis in [validation, share]:
        axis.set_xlabel('Simulated seconds')
    timing.set_xlabel('Completed rounds')
    share.set_ylim(0, 1)
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=160)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage', choices=('smoke', 'pilot', 'compare', 'full'),
                        default='compare')
    parser.add_argument('--profile', choices=('H', 'X', 'D'), default='H')
    parser.add_argument('--mbps', type=int, choices=(10, 30, 100), default=30)
    parser.add_argument('--seed', type=int, default=12345)
    parser.add_argument('--rounds', type=int)
    parser.add_argument('--show', action=argparse.BooleanOptionalAction,
                        default=os.name == 'nt',
                        help='Open the generated PNG in the Windows image viewer')
    args = parser.parse_args()
    rounds = args.rounds if args.rounds is not None else (500 if args.stage == 'full' else 10)
    condition = f'{args.profile}_{args.mbps}mbps_s{args.seed}_r{rounds}'
    root = Path(__file__).resolve().parents[1] / 'exp' / 'traffic' / args.stage
    output = root / f'plots_{condition}.png'
    plot(load_results(root / 'femnist', condition),
         f'FEMNIST · {args.profile} · {args.mbps} Mbps · seed {args.seed} · {rounds} rounds',
         output)
    print(output)
    if args.show and os.name == 'nt':
        os.startfile(output)


if __name__ == '__main__':
    main()
