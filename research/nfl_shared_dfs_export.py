"""Generate a separate shared DFS candidate from frozen full event-model inputs."""
import argparse
from pathlib import Path
from hashlib import sha256
from model.nfl_shared_matchup_scenarios import build_coherent_banks
from model.nfl_longest_touchdown import timestamp
from research.nfl_longest_touchdown import read, write


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True, help='Frozen build_coherent_banks keyword inputs')
    parser.add_argument('--role-evidence', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--retrospective', action='store_true')
    args = parser.parse_args()
    inputs = read(args.input)
    manifest = inputs.get('source_manifest', {})
    if not manifest.get('captured_at'):
        raise ValueError('Frozen inputs require a source capture timestamp')
    if timestamp(manifest['captured_at']) > timestamp(inputs['decision_at']) and not args.retrospective:
        raise ValueError('Later source capture requires explicit retrospective mode')
    result = build_coherent_banks(**inputs, role_dispersion_evidence=read(args.role_evidence) if args.role_evidence else None)
    result.update(input_file_sha256=sha256(args.input.read_bytes()).hexdigest(), retrospective=args.retrospective)
    if args.retrospective:
        result['limitations'].append('Later source replay, not archived pregame availability or forward validation.')
    write(args.output, result)


if __name__ == '__main__':
    main()
