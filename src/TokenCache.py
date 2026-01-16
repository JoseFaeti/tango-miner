import gzip
import json
import hashlib
import unicodedata
from datetime import datetime
from pathlib import Path

class TokenCache:
    def __init__(self, cache_dir: Path, tokenizer_fingerprint: str):
        self.cache_dir = cache_dir
        self.fingerprint = tokenizer_fingerprint
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # separate index for mtime → content hash
        self.mtime_index_path = self.cache_dir / "mtime_index.json"

        if self.mtime_index_path.exists():
            try:
                self._mtime_index = json.loads(self.mtime_index_path.read_text("utf-8"))
            except Exception:
                self._mtime_index = {}
        else:
            self._mtime_index = {}

    # ---------------- core helpers ----------------

    def _normalize_text(self, text: str) -> str:
        text = unicodedata.normalize("NFKC", text)
        return text.replace("\r\n", "\n").replace("\r", "\n")

    def _compute_key(self, normalized_text: str) -> str:
        h = hashlib.sha256()
        h.update(normalized_text.encode("utf-8"))
        h.update(b"\0")
        h.update(self.fingerprint.encode("utf-8"))
        return h.hexdigest()

    def _cache_path(self, key: str) -> Path:
        return self.cache_dir / f"{key}.json.gz"

    def _flush_mtime_index(self):
        self.mtime_index_path.write_text(
            json.dumps(self._mtime_index, ensure_ascii=False),
            encoding="utf-8",
        )

    # ---------------- hash-based cache ----------------

    def get(self, text: str):
        normalized = self._normalize_text(text)
        key = self._compute_key(normalized)
        path = self._cache_path(key)

        if not path.exists():
            return None

        try:
            with gzip.open(path, "rt", encoding="utf-8") as f:
                payload = json.load(f)
            return payload["tokens"]
        except (json.JSONDecodeError, EOFError, OSError):
            path.unlink(missing_ok=True)
            return None

    def put(self, text: str, tokens):
        normalized = self._normalize_text(text)
        key = self._compute_key(normalized)
        path = self._cache_path(key)

        payload = {
            "tokenizer_fingerprint": self.fingerprint,
            "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "token_count": len(tokens),
            "tokens": tokens,
        }

        with gzip.open(path, "wt", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)

        return key  # important: return hash for mtime index

    # ---------------- mtime-based shortcut ----------------

    def get_by_mtime(self, path: Path, mtime_ns: int):
        key = str(path.resolve())
        entry = self._mtime_index.get(key)

        if not entry or entry["mtime"] != mtime_ns:
            return None

        cache_path = self._cache_path(entry["hash"])
        if not cache_path.exists():
            return None

        try:
            with gzip.open(cache_path, "rt", encoding="utf-8") as f:
                payload = json.load(f)
            return payload["tokens"]
        except Exception:
            return None

    def put_by_mtime(self, path: Path, mtime_ns: int, text: str, tokens):
        normalized = self._normalize_text(text)
        content_hash = self._compute_key(normalized)

        # store token blob
        self.put(text, tokens)

        # update index
        self._mtime_index[str(path.resolve())] = {
            "mtime": mtime_ns,
            "hash": content_hash,
        }
        self._flush_mtime_index()
