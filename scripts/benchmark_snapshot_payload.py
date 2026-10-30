"""Measure a CSV-only partial proxy for unchanged-run snapshot serialization.

The payload is not a usable result snapshot: it omits WordStats, provenance,
and Anki inputs. This script only reads an existing completed run's CSVs.
"""
import argparse
import hashlib
import json
from pathlib import Path
from time import perf_counter


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run', type=Path)
    parser.add_argument('--repeat', type=int, default=3)
    args = parser.parse_args()
    if args.repeat < 1:
        parser.error('--repeat must be positive')
    summary = json.loads((args.run / 'summary.json').read_text(encoding='utf-8'))
    if summary.get('status') != 'completed':
        parser.error('run must be completed')
    paths = sorted(args.run.glob('*.csv'))
    if len(paths) != 3:
        parser.error('expected three run output CSV files')
    start = perf_counter()
    payload = {p.name: p.read_bytes().decode('utf-8') for p in paths}
    print(f'Files: {len(paths)}; raw bytes: {sum(p.stat().st_size for p in paths)}; read: {perf_counter() - start:.3f}s')
    for index in range(args.repeat):
        start = perf_counter()
        encoded = json.dumps(payload, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
        digest = hashlib.sha256(encoded).digest()
        encoded_time = perf_counter() - start
        start = perf_counter()
        if hashlib.sha256(encoded).digest() != digest:
            raise ValueError('checksum mismatch')
        restored = json.loads(encoded)
        decoded_time = perf_counter() - start
        if restored != payload:
            raise ValueError('round trip mismatch')
        print(f'{index + 1}: encode+checksum {encoded_time:.3f}s; verify+decode {decoded_time:.3f}s; bytes {len(encoded)}')
    print('CSV-only partial proxy; excludes complete result state, filesystem writes, identity checks and Anki.')


if __name__ == '__main__':
    main()
