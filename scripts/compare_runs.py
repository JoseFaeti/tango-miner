"""Compare archived mining results without contacting Anki or rerunning mining."""

import argparse
import csv
import html
import json
import re
import sys
from pathlib import Path

ARCHIVE = re.compile(r"^(\d{4}-\d{2}-\d{2}_\d{4})_[^/]+?(?:_(\d+))?$")


def load_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def completed_runs(root):
    runs = []
    for path in Path(root).iterdir():
        match = ARCHIVE.fullmatch(path.name)
        if not path.is_dir() or not match:
            continue
        try:
            summary = load_json(path / "summary.json")
        except (OSError, ValueError):
            continue
        if summary.get("status") == "completed":
            runs.append((match.group(1), int(match.group(2) or 1), path.name, path))
    return [item[-1] for item in sorted(runs)]


def load_run(path):
    path = Path(path)
    summary = load_json(path / "summary.json")
    if summary.get("status") != "completed":
        raise ValueError(f"Run is not completed: {path}")
    csvs = [p for p in path.glob("*.csv")
            if not p.name.endswith((".dropped.csv", ".sentence.csv"))]
    if len(csvs) != 1:
        raise ValueError(f"Expected exactly one main CSV in {path}, found {len(csvs)}")
    main = csvs[0]
    dropped = main.with_name(main.stem + ".dropped" + main.suffix)
    rows = {}
    for file, status in ((main, "valid"), (dropped, "dropped/review")):
        with file.open(encoding="utf-8-sig", newline="") as stream:
            for row in csv.reader(stream):
                if len(row) != 9:
                    raise ValueError(f"Unexpected row in {file}: expected 9 columns")
                word = row[1]
                if word in rows:
                    raise ValueError(f"Duplicate candidate {word!r} in {path}")
                rows[word] = {"status": status, "frequency": int(row[4]),
                              "definition": row[5], "examples": row[8]}
    return {"path": path, "summary": summary, "rows": rows,
            "config": load_json(path / "config.json"),
            "manifest": load_json(path / "input-manifest.json")}


def describe(candidate):
    if candidate is None:
        return "absent"
    return f"{candidate['status']}, frequency {candidate['frequency']}"


def plain_examples(value):
    value = re.sub(r"<br\s*/?>", "\n", value, flags=re.I)
    return html.unescape(re.sub(r"<[^>]*>", "", value))


def report(before, after, words, limit):
    a, b = before["rows"], after["rows"]
    print(f"Before: {before['path']}\nAfter:  {after['path']}")
    print(f"Configuration identical: {before['config'] == after['config']}")
    print(f"Input manifest identical: {before['manifest'] == after['manifest']}")
    for name, run in (("Before", before), ("After", after)):
        counts = {status: sum(v['status'] == status for v in run['rows'].values())
                  for status in ("valid", "dropped/review")}
        print(f"{name}: {counts['valid']} valid, {counts['dropped/review']} dropped/review")
        print(f"  Anki outcome: {json.dumps(run['summary'].get('anki', {}), ensure_ascii=False)}")
    added, removed = sorted(b.keys() - a.keys()), sorted(a.keys() - b.keys())
    print(f"\nAdded candidates ({len(added)}): {', '.join(added[:limit]) or 'none'}")
    print(f"Removed candidates ({len(removed)}): {', '.join(removed[:limit]) or 'none'}")
    transitions = sorted(w for w in a.keys() & b.keys() if a[w]['status'] != b[w]['status'])
    print(f"Status changes ({len(transitions)}):")
    for word in transitions[:limit]:
        print(f"  {word}: {a[word]['status']} -> {b[word]['status']}")
    changed = [(w, b[w]['frequency'] - a[w]['frequency']) for w in a.keys() & b.keys()
               if a[w]['frequency'] != b[w]['frequency']]
    changed.sort(key=lambda item: (-abs(item[1]), item[0]))
    print(f"Largest frequency changes ({len(changed)} total):")
    for word, delta in changed[:limit]:
        print(f"  {word}: {a[word]['frequency']} -> {b[word]['frequency']} ({delta:+d})")
    for word in words:
        print(f"\n{word}:")
        for name, rows in (("Before", a), ("After", b)):
            item = rows.get(word)
            print(f"  {name}: {describe(item)}")
            if item:
                print(f"    Definition: {plain_examples(item['definition'])}")
                print(f"    Examples: {plain_examples(item['examples']).replace(chr(10), ' | ')}")
    print("\nCandidate differences do not identify Anki note deletions or prove correctness.")


def compact_report(before, after, words, limit=5):
    """Print bounded comparison summaries and selected candidates on one line each."""
    a, b = before['rows'], after['rows']
    print(f"Before: {before['path']}")
    print(f"After:  {after['path']}")
    print(f"Inputs identical: {before['manifest'] == after['manifest']}; config identical: {before['config'] == after['config']}")
    for label, run in [('before', before), ('after', after)]:
        valid = sum(v['status'] == 'valid' for v in run['rows'].values())
        print(f"{label}: {valid} valid, {len(run['rows']) - valid} dropped/review; Anki: {run['summary'].get('anki', {}).get('status', 'unknown')}")
    added, removed = sorted(b.keys() - a.keys()), sorted(a.keys() - b.keys())
    transitions = [w for w in a.keys() & b.keys() if a[w]['status'] != b[w]['status']]
    frequency_changes = sum(a[w]['frequency'] != b[w]['frequency'] for w in a.keys() & b.keys())
    print(f"Added: {len(added)}; removed: {len(removed)}; status changes: {len(transitions)}; frequency changes: {frequency_changes}")
    for label, values in [('Added', added), ('Removed', removed)]:
        if values and limit:
            print(f"{label} (first {min(limit, len(values))}): {', '.join(values[:limit])}")
    for word in words:
        print(f"{word}: {describe(a.get(word))} -> {describe(b.get(word))}")
    print('Dropped/review combines invalid and unresolved; candidate changes do not prove Anki changes.')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("before", nargs="?", type=Path, help="older run directory")
    parser.add_argument("after", nargs="?", type=Path, help="newer run directory")
    parser.add_argument("--runs-dir", type=Path, default=Path("runs"), help="archive root for automatic selection")
    parser.add_argument("--word", action="append", default=[], help="inspect a candidate; repeat as needed")
    parser.add_argument("--limit", type=int, default=20, help="maximum entries displayed per summary list")
    parser.add_argument("--compact", action="store_true", help="bounded summary without definitions or examples")
    args = parser.parse_args(argv)
    if (args.before is None) != (args.after is None):
        parser.error("provide both run directories or neither")
    if args.limit < 0:
        parser.error("--limit must be non-negative")
    try:
        if args.before is None:
            runs = completed_runs(args.runs_dir)
            if len(runs) < 2:
                raise ValueError(f"Fewer than two completed runs in {args.runs_dir}")
            args.before, args.after = runs[-2:]
        before, after = load_run(args.before), load_run(args.after)
        (compact_report if args.compact else report)(before, after, args.word, args.limit)
    except (OSError, ValueError, KeyError, csv.Error) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
