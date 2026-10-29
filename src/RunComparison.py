"""Read and compare archived CSV outputs without mining or contacting Anki."""
import csv
import json
import re
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



def build_change_report(before, after):
    changes = []
    old = before["rows"] if before else {}
    new = after["rows"]
    if before is not None:
        for word in sorted(old.keys() | new.keys()):
            a, b = old.get(word), new.get(word)
            if a is None or b is None:
                changes.append({"candidate": word, "type": "added" if a is None else "removed",
                                "before": a, "after": b})
            else:
                fields = {key: {"before": a[key], "after": b[key]}
                          for key in ("status", *FIELDS) if a[key] != b[key]}
                if fields:
                    changes.append({"candidate": word, "type": "changed", "fields": fields})
    counts = {key: sum(item["type"] == key for item in changes)
              for key in ("added", "removed", "changed")}
    counts["status_changes"] = sum("status" in item.get("fields", {}) for item in changes)
    return {"schema_version": 1, "status": "compared" if before else "no_baseline",
            "baseline": before["path"].name if before else None,
            "current": after["path"].name,
            "inputs_identical": before["manifest"] == after["manifest"] if before else None,
            "config_identical": before["config"] == after["config"] if before else None,
            "counts": counts, "changes": changes}


def write_change_report(path, report):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)
