# Legacy module name retained for import compatibility.
# ruff: noqa: N999
import hashlib
import json
import os
import pickle
import tempfile
from pathlib import Path

from src.CachePaths import remove_legacy_entries


class _TokenUnpickler(pickle.Unpickler):
    def find_class(self, module, name):
        raise pickle.UnpicklingError("Cache globals are not allowed")


class TokenCache:
    """Cache raw tokens by the ordered normalized sentence list."""

    def __init__(self, cache_dir: Path, tokenizer_fingerprint: str):
        self.cache_dir = cache_dir / 'token-v3'
        self.fingerprint = tokenizer_fingerprint
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _compute_key(self, sentences):
        data = json.dumps([self.fingerprint, sentences], ensure_ascii=False, separators=(',', ':')).encode('utf-8')
        return hashlib.sha256(data).hexdigest()

    def _path(self, source):
        identity = str(Path(source).resolve()) if source is not None else ''
        return self.cache_dir / (hashlib.sha256(identity.encode('utf-8')).hexdigest() + '.pkl')

    def load(self, sentences, source=None):
        path = self._path(source)
        try:
            with path.open('rb') as file:
                payload = _TokenUnpickler(file).load()
            tokens = payload['tokens']
            if (payload['version'] != 2 or payload['key'] != self._compute_key(sentences) or payload['fingerprint'] != self.fingerprint
                    or not isinstance(tokens, list) or len(tokens) != len(sentences)):
                return None
            for stream in tokens:
                if not isinstance(stream, list):
                    return None
                for token in stream:
                    if (not isinstance(token, dict) or not all(isinstance(token.get(k), str)
                        for k in ('surface', 'lemma', 'base_form', 'reading'))
                        or not isinstance(token.get('pos'), (tuple, list))
                        or not all(isinstance(p, str) for p in token['pos'])):
                        return None
                    token['pos'] = tuple(token['pos'])
            return tokens
        except (OSError, ValueError, KeyError, TypeError, EOFError, pickle.UnpicklingError, OverflowError):
            return None

    def put(self, sentences, tokens, source=None):
        path = self._path(source)
        payload = {'version': 2, 'key': self._compute_key(sentences), 'fingerprint': self.fingerprint, 'tokens': tokens}
        name = None
        try:
            with tempfile.NamedTemporaryFile(mode='wb', dir=self.cache_dir, delete=False) as file:
                name = file.name
                pickle.dump(payload, file, protocol=pickle.HIGHEST_PROTOCOL)
            os.replace(name, path)
            remove_legacy_entries(self.cache_dir.parent / 'token-v2', '.pkl')
        finally:
            if name and os.path.exists(name):
                os.unlink(name)
