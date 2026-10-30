"""Create an auditable, non-overwriting output directory for each run."""
import json
import subprocess
from datetime import datetime
from pathlib import Path


def _git(repo, *args, allow_difference=False):
    result = subprocess.run(["git", "-C", str(repo), *args], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, check=False)
    if result.returncode not in ((0, 1) if allow_difference else (0,)):
        raise RuntimeError(result.stderr.decode("utf-8", errors="replace").strip())
    return result.stdout


def create_run_archive(output_path, config, *, repo=None, now=None):
    """Save run configuration and the tracked/untracked Git changes before mining."""
    output_path = Path(output_path)
    repo = Path(repo) if repo is not None else Path(__file__).resolve().parent.parent
    patch = b""
    warning = None
    try:
        revision = _git(repo, "rev-parse", "--verify", "HEAD").decode().strip()
        status = _git(repo, "status", "--porcelain=v1", "-z", "--untracked-files=all")
        dirty = bool(status)
        if dirty:
            patch = _git(repo, "diff", "--binary", "HEAD", "--")
            untracked = _git(repo, "ls-files", "--others", "--exclude-standard", "-z")
            for filename in untracked.split(b"\0"):
                if filename:
                    name = filename.decode("utf-8", errors="surrogateescape")
                    patch += _git(repo, "diff", "--no-index", "--binary", "--", "/dev/null", name,
                                  allow_difference=True)
        identifier = revision[:7] + ("-dirty" if dirty else "")
    except (OSError, RuntimeError) as exc:
        revision, identifier = None, "unknown"
        warning = str(exc)

    timestamp = (now or datetime.now()).strftime("%Y-%m-%d_%H%M")
    root = output_path.parent / "runs"
    root.mkdir(parents=True, exist_ok=True)
    base = f"{timestamp}_{identifier}"
    run_dir = root / base
    counter = 2
    while True:
        try:
            run_dir.mkdir()
            break
        except FileExistsError:
            run_dir = root / f"{base}_{counter}"
            counter += 1
    (run_dir / "config.json").write_text(config.to_json() + "\n", encoding="utf-8")
    if revision and dirty:
        (run_dir / "changes.patch").write_bytes(patch)
    notes = [f"Git revision: {revision or 'unavailable'}", f"Input: {config.input.path}",
             "Input file hashes: input-manifest.json", "Notes: "]
    if warning:
        notes.append(f"Git snapshot unavailable: {warning}")
    (run_dir / "notes.txt").write_text("\n".join(notes) + "\n", encoding="utf-8")
    return run_dir / output_path.name


def write_input_manifest(files, destination):
    import hashlib
    entries = []
    for file in files:
        digest = hashlib.sha256()
        with Path(file).open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        entries.append({"path": str(Path(file).resolve()), "sha256": digest.hexdigest()})
    Path(destination).write_text(json.dumps(entries, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _file_identity(path):
    import hashlib
    path = Path(path).expanduser().resolve()
    result = {"path": str(path)}
    try:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        result.update(size_bytes=path.stat().st_size, sha256=digest.hexdigest())
    except OSError as exc:
        result["error"] = str(exc)
    return result


def collect_dependencies(config):
    """Record versions and actual dictionary/config file hashes where discoverable."""
    import importlib.metadata as metadata
    import platform
    result = {"python": platform.python_version(), "packages": {}, "files": {}}
    names = ["SudachiPy"]
    dictionary = config.tokenizer.dictionary
    package = {"full": "SudachiDict-full", "core": "SudachiDict-core", "small": "SudachiDict-small"}.get(dictionary.lower())
    if package:
        names.append(package)
    for name in names:
        try:
            dist = metadata.distribution(name)
            result["packages"][name] = dist.version
            if name == package:
                files = [dist.locate_file(f) for f in (dist.files or []) if str(f).endswith("system.dic")]
                result["files"]["sudachi_dictionary"] = [_file_identity(f) for f in files]
                if not files:
                    result["files"]["sudachi_dictionary"] = {"error": "Dictionary file not found in package metadata"}
        except metadata.PackageNotFoundError:
            result["packages"][name] = None
    if not package:
        result["files"]["sudachi_dictionary"] = [_file_identity(dictionary)]
    result["files"]["jmdict"] = _file_identity(config.dictionary.jmdict_path)
    config_path = Path(config.tokenizer.config_path).expanduser()
    result["files"]["sudachi_config"] = _file_identity(config_path)
    try:
        settings = json.loads(config_path.read_text(encoding="utf-8"))
        extra = list(settings.get("userDict", []))
        if settings.get("systemDict"):
            extra.append(settings["systemDict"])
        result["files"]["configured_sudachi_dictionaries"] = [_file_identity(path) for path in extra]
    except (OSError, ValueError, TypeError) as exc:
        result["files"]["configured_sudachi_dictionaries"] = {"error": str(exc)}
    return result


def write_run_summary(directory, summary):
    """Write a summary atomically so a partial write is not mistaken for success."""
    target = Path(directory) / "summary.json"
    temporary = target.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(target)


def write_stage_record(directory, index, step, record):
    """Persist a compact ordered stage report and available archive artifacts."""
    import re
    root = Path(directory)
    stage_dir = root / "steps"
    stage_dir.mkdir(parents=True, exist_ok=True)
    name = re.sub(r"[^A-Za-z0-9_-]", "_", record["name"])
    target = stage_dir / f"{index:02d}-{name}.json"
    report = {"schema_version": 1, "step_number": index, **record}
    paths = []
    for attribute in ("manifest_path", "path", "diagnostics_path", "plan_path", "output_path"):
        value = getattr(step, attribute, None)
        if value is None:
            continue
        path = Path(value)
        candidates = [path]
        if attribute == "output_path":
            candidates += [path.with_name(path.stem + suffix + path.suffix)
                           for suffix in (".dropped", ".sentence")]
        for candidate in candidates:
            try:
                relative = candidate.resolve().relative_to(root.resolve())
            except ValueError:
                continue
            if candidate.is_file():
                paths.append(relative.as_posix())
    if paths:
        report["artifacts"] = sorted(set(paths))
    temporary = target.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(target)


def write_performance(directory, steps):
    """Write new measurements without duplicating stage records."""
    target = Path(directory) / "performance.json"
    temporary = target.with_suffix(".json.tmp")
    report = {"schema_version": 3, "steps": steps}
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(target)
