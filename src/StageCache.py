"""Cache deterministic stage results without putting cache logic in steps."""
import hashlib
from dataclasses import replace
import json
import sys
import tempfile
from itertools import groupby
from pathlib import Path

from src.Artifact import Artifact
from src.TokenizedSentence import TokenizedSentence
from src.TokenProvenance import TokenComponent
from src.steps.MergeTokensStep import MergeTokensStep


def _pack_token(token):
    values = dict(token)
    pos = values.pop("pos")
    components = values.pop("components", None)
    if not isinstance(pos, (list, tuple)) or any(not isinstance(p, str) for p in pos):
        raise TypeError("Unsupported token POS")
    if any(not isinstance(v, (str, int, float, bool, type(None))) for v in values.values()):
        raise TypeError("Unsupported token field")
    encoded = None if components is None else [
        [c.surface, c.lemma, c.base_form, c.reading, list(c.pos)] for c in components
    ]
    return [values, pos, isinstance(pos, tuple), encoded]


def _unpack_token(value):
    values, pos, pos_tuple, components = value
    if not isinstance(values, dict) or not isinstance(pos, list) or not isinstance(pos_tuple, bool):
        raise ValueError("Invalid cached token")
    token = dict(values)
    token["pos"] = tuple(pos) if pos_tuple else pos
    if components is not None:
        token["components"] = tuple(TokenComponent(a, b, c, d, tuple(p))
                                    for a, b, c, d, p in components)
    return token


def _input_key_data(sentences):
    """Encode raw inputs compactly while preserving POS and source identities."""
    result = []
    for sentence in sentences:
        raw = []
        for token in sentence.tokens:
            # Normal tokenizer output is JSON-native apart from POS tuples.
            # Fall back to the explicit encoding for unusual token structures.
            if "components" in token or any(not isinstance(v, (str, int, float, bool, type(None)))
                                              for k, v in token.items() if k != "pos"):
                raw.append([1, _pack_token(token)])
            else:
                pos = token["pos"]
                if not isinstance(pos, (tuple, list)) or any(not isinstance(p, str) for p in pos):
                    raise TypeError("Unsupported token POS")
                raw.append([0, token, isinstance(pos, tuple)])
        result.append([sentence.text, sentence.tag,
                       str(sentence.origin) if sentence.origin is not None else None, raw])
    return result


def _pack_changes(original, merged):
    if len(original) != len(merged):
        raise ValueError("Merge changed sentence count")
    changes = []
    for source, result in zip(original, merged):
        if (source.text, source.tag, source.origin) != (result.text, result.tag, result.origin):
            raise ValueError("Merge changed sentence metadata")
        if source.tokens == result.tokens:
            changes.append(None)
            continue
        positions = {id(token): i for i, token in enumerate(source.tokens)}
        changes.append([positions[id(token)] if id(token) in positions else _pack_token(token)
                        for token in result.tokens])
    return changes


def _unpack_changes(original, changes):
    if not isinstance(changes, list) or len(changes) != len(original):
        raise ValueError("Invalid cached sentence count")
    result = []
    for sentence, tokens in zip(original, changes):
        if tokens is None:
            result.append(sentence)
            continue
        if not isinstance(tokens, list):
            raise ValueError("Invalid cached sentence")
        restored = []
        for token in tokens:
            if type(token) is int:
                if token < 0 or token >= len(sentence.tokens):
                    raise ValueError("Invalid cached token reference")
                restored.append(sentence.tokens[token])
            else:
                restored.append(_unpack_token(token))
        result.append(TokenizedSentence(sentence.text, sentence.tag, sentence.origin, restored))
    return result


def _bytes(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")


# Cache correctness depends on these modules. Update this list if merge logic
# starts calling another project module; upstream token changes are in the key.
MERGE_SOURCE_DEPENDENCIES = (
    "StageCache.py", "Artifact.py", "JMDict.py", "PipelineStep.py",
    "TokenizedSentence.py", "TokenProvenance.py",
    "steps/MergeTokensStep.py", "steps/ProcessingStep.py",
)


def _source_fingerprint(root=None):
    """Fingerprint merge and its project helpers, excluding unrelated stages."""
    root = Path(root) if root is not None else Path(__file__).resolve().parent
    digest = hashlib.sha256()
    for name in MERGE_SOURCE_DEPENDENCIES:
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update((root / name).read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


class StageCache:
    """Wrap merging per contiguous source; all other stages run normally."""

    def __init__(self, directory, dependencies, refresh=()):
        self.directory = Path(directory) / "stages" / "merge-v2"
        self.refresh = set(refresh)
        self.dictionary_hash = dependencies.get("files", {}).get("jmdict", {}).get("sha256")
        try:
            self.source_hash = _source_fingerprint()
        except OSError:
            self.source_hash = None
        self.stats = None

    def process(self, step, artifact):
        if not isinstance(step, MergeTokensStep):
            return step.process(artifact)
        self.stats = {"stage": "merge", "unit": "source groups", "hits": 0, "misses": 0, "refreshes": 0,
                      "hit_streams": 0, "miss_streams": 0, "refreshed_streams": 0, "write_failures": 0}
        if step.diagnostics_path is not None or not self.dictionary_hash or not self.source_hash:
            self.stats["disabled_reason"] = ("diagnostics" if step.diagnostics_path is not None
                                             else "dictionary hash unavailable" if not self.dictionary_hash
                                             else "source fingerprint unavailable")
            handler = step._progress_handler
            if handler:
                def report(event):
                    if event.current >= event.total:
                        event = replace(event, message=f"{event.message} Cache disabled: {self.stats['disabled_reason']}.")
                    handler(event)
                step._progress_handler = report
            try:
                return step.process(artifact)
            finally:
                step._progress_handler = handler

        output = []
        def compute(group):
            handler = step._progress_handler
            step._progress_handler = None
            try:
                return step.process(Artifact(group, tmpdir=artifact.tmpdir)).data
            finally:
                step._progress_handler = handler

        for _, sentences in groupby(artifact.data, key=lambda sentence: sentence.origin):
            group = list(sentences)
            try:
                key_data = ["merge-v2", sys.version, self.source_hash, self.dictionary_hash, step.max_lookahead, _input_key_data(group)]
                key = hashlib.sha256(_bytes(key_data)).hexdigest()
            except (TypeError, ValueError, KeyError, AttributeError, RecursionError):
                self.stats["misses"] += 1
                self.stats["miss_streams"] += len(group)
                output.extend(compute(group))
                continue
            path = self.directory / (key + ".json")
            forced = bool(self.refresh & {"merge", "all"})
            cached = None if forced else self._load(path, key, group)
            if cached is None:
                self.stats["refreshes" if forced else "misses"] += 1
                self.stats["refreshed_streams" if forced else "miss_streams"] += len(group)
                merged = compute(group)
                if not self._save(path, key, group, merged):
                    self.stats["write_failures"] += 1
            else:
                self.stats["hits"] += 1
                self.stats["hit_streams"] += len(group)
                merged = cached
            output.extend(merged)
            if step._progress_handler and len(output) < len(artifact.data):
                step.progress(len(output), len(artifact.data))
        if step._progress_handler:
            step.done(
                f"{len(output)} token streams merged; cache: {self.stats['hits']} hits, "
                f"{self.stats['misses']} misses, {self.stats['refreshes']} refreshed "
                f"(source groups; {self.stats['hit_streams']} cached streams)."
            )
        return Artifact(output)

    def _load(self, path, key, original):
        try:
            entry = json.loads(path.read_text(encoding="utf-8"))
            if entry["key"] != key or entry["checksum"] != hashlib.sha256(_bytes(entry["data"])).hexdigest():
                return None
            result = _unpack_changes(original, entry["data"])
            if not isinstance(result, list) or not all(isinstance(s, TokenizedSentence) for s in result):
                return None
            return result
        except (OSError, ValueError, TypeError, KeyError, IndexError, AttributeError, RecursionError):
            return None

    def _save(self, path, key, original, result):
        temporary = None
        try:
            data = _pack_changes(original, result)
            entry = {"key": key, "checksum": hashlib.sha256(_bytes(data)).hexdigest(), "data": data}
            path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
                temporary = Path(stream.name)
                stream.write(_bytes(entry))
            temporary.replace(path)
            return True
        except (OSError, TypeError, ValueError, KeyError, AttributeError, RecursionError):
            return False  # The computed result remains usable.
        finally:
            if temporary is not None:
                try:
                    temporary.unlink(missing_ok=True)
                except OSError:
                    pass
