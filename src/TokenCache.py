import json
import hashlib
import unicodedata
import pickle
from datetime import datetime
from pathlib import Path


class TokenCache:
    def __init__(self, cache_dir: Path, tokenizer_fingerprint: str):
        self.cache_dir = cache_dir
        self.fingerprint = tokenizer_fingerprint
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.mtime_index_path = self.cache_dir / "mtime_index.json"

        if self.mtime_index_path.exists():
            try:
                self._mtime_index = json.loads(self.mtime_index_path.read_text("utf-8"))
            except Exception:
                self._mtime_index = {}
        else:
            self._mtime_index = {}

        self._mtime_dirty = False


    # ---------------- helpers ----------------

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
        return self.cache_dir / f"{key}.pkl"

    def flush_mtime_index(self):
        if not self._mtime_dirty:
            return

        self.mtime_index_path.write_text(
            json.dumps(self._mtime_index, ensure_ascii=False),
            encoding="utf-8",
        )
        self._mtime_dirty = False


    # ---------------- hash-based cache ----------------

    def load_by_hash(self, key: str):
        path = self._cache_path(key)
        if not path.exists():
            return None

        try:
            with open(path, "rb") as f:
                return pickle.load(f)
        except Exception:
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

        with open(path, "wb") as f:
            pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)

        return key


    # ---------------- mtime-based shortcut ----------------

    def get_hash_by_mtime(self, path: Path, mtime_ns: int):
        entry = self._mtime_index.get(str(path.resolve()))
        if entry and entry["mtime"] == mtime_ns:
            return entry["hash"]
        return None

    def put_by_mtime(self, path: Path, mtime_ns: int, text: str, tokens):
        normalized = self._normalize_text(text)
        content_hash = self._compute_key(normalized)

        self.put(text, tokens)

        self._mtime_index[str(path.resolve())] = {
            "mtime": mtime_ns,
            "hash": content_hash,
        }
        self._mtime_dirty = True
