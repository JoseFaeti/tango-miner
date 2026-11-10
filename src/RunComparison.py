"""Read and compare archived CSV outputs without mining or contacting Anki."""
import csv
import html
import json
import re
from itertools import zip_longest
from pathlib import Path

FIELDS = ("score", "word", "reading", "index", "frequency", "definition", "tags", "sentence_count", "examples")

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


def load_run(path, *, require_completed=True):
    path = Path(path)
    summary = load_json(path / "summary.json")
    if require_completed and summary.get("status") != "completed":
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
                rows[word] = {"status": status, **dict(zip(FIELDS, row))}
                rows[word]["frequency"] = int(row[4])
    return {"path": path, "summary": summary, "rows": rows,
            "config": load_json(path / "config.json"),
            "manifest": load_json(path / "input-manifest.json")}



def report_examples(value):
    if not value or value == "no sentences :(":
        return []
    result = []
    for part in re.split(r"<br\s*/?>\s*<br\s*/?>", value, flags=re.I):
        match = re.fullmatch(
            r"(.*?)<br\s*/?>\s*<span\s+class=['\"]tag sentence-tag['\"]>.*? - ([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)</span>",
            part, flags=re.I | re.S)
        if not match:
            raise ValueError(f"Malformed example: {part!r}")
        text = re.sub(r"<(?!/?span\b)[^>]*>", "", match.group(1), flags=re.I)
        result.append({"text": html.unescape(text), "score": float(match.group(2))})
    return result


def report_row(row):
    count = re.fullmatch(r"sentences:\s*(\d+)", row["sentence_count"].strip())
    if not count:
        raise ValueError(f"Malformed sentence count: {row['sentence_count']!r}")
    return {**{key: row[key] for key in ("reading", "frequency", "definition", "tags")},
            "score": float(row["score"]), "index": float(row["index"]),
            "sentence_count": int(count.group(1)), "examples": report_examples(row["examples"])}


def example_changes(before, after):
    changes = []
    for old, new in zip_longest(before, after):
        old = old if old is not None else {"text": None, "score": None}
        new = new if new is not None else {"text": None, "score": None}
        changes.append({key: [old[key]] if old[key] == new[key] else [old[key], new[key]]
                        for key in ("text", "score")})
    return changes


def unwrap_single_values(value):
    if isinstance(value, dict):
        return {key: unwrap_single_values(item) for key, item in value.items()}
    if isinstance(value, list):
        items = [unwrap_single_values(item) for item in value]
        return items[0] if len(items) == 1 else items
    return value


def build_change_report(before, after):
    changes = {key: [] for key in ("added", "removed", "changed")}
    old = {word: report_row(row) for word, row in before["rows"].items() if row["status"] == "valid"} if before else {}
    new = {word: report_row(row) for word, row in after["rows"].items() if row["status"] == "valid"}
    if before is not None:
        for word in sorted(old.keys() | new.keys()):
            a, b = old.get(word), new.get(word)
            if a is None or b is None:
                category = "added" if a is None else "removed"
                changes[category].append({"candidate": word, **(b if a is None else a)})
            else:
                fields = {key: [a[key], b[key]]
                          for key in a if key != "examples" and a[key] != b[key]}
                if a["examples"] != b["examples"]:
                    fields["examples"] = example_changes(a["examples"], b["examples"])
                if fields:
                    changes["changed"].append({"candidate": word, "fields": fields})
    counts = {key: len(items) for key, items in changes.items()}
    return unwrap_single_values({"schema_version": 3, "status": "compared" if before else "no_baseline",
            "baseline": (before["path"].name if before["path"].resolve().parent == after["path"].resolve().parent else str(before["path"].resolve())) if before else None,
            "current": after["path"].name,
            "inputs_identical": before["manifest"] == after["manifest"] if before else None,
            "config_identical": before["config"] == after["config"] if before else None,
            "counts": counts, "changes": changes})


def write_change_report(path, report):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)
