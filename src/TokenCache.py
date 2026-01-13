import hashlib
import json
import gzip
import unicodedata
from pathlib import Path
from datetime import datetime


class TokenCache:
    def __init__(self, cache_dir: Path, tokenizer_fingerprint: str):
        self.cache_dir = cache_dir
        self.fingerprint = tokenizer_fingerprint
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _normalize_text(self, text: str) -> str:
        text = unicodedata.normalize("NFKC", text)
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        return text

    def _compute_key(self, normalized_text: str) -> str:
        h = hashlib.sha256()
        h.update(normalized_text.encode("utf-8"))
        h.update(b"\0")
        h.update(self.fingerprint.encode("utf-8"))
        return h.hexdigest()

    def _cache_path(self, key: str) -> Path:
        return self.cache_dir / f"{key}.json.gz"

    def get(self, text: str) -> list[dict] | None:
        normalized = self._normalize_text(text)
        key = self._compute_key(normalized)
        path = self._cache_path(key)

        # print(f'Cache: key={key}, path={path}')

        if not path.exists():
            # print('Cache path does not exist!')
            return None

        with gzip.open(path, "rt", encoding="utf-8") as f:
            payload = json.load(f)
            # print(f'Found cache: {payload}')

        return payload["tokens"]

    def put(self, text: str, tokens: list[dict]) -> None:
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
