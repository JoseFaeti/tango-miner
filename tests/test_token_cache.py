import tempfile
import unittest
from pathlib import Path

from src.TokenCache import TokenCache


class TokenCacheTests(unittest.TestCase):
    def test_put_and_load_by_hash_round_trips_tokens(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache = TokenCache(Path(tmp), "fingerprint")
            tokens = [{"surface": "猫", "lemma": "猫"}]

            key = cache.put("猫\r\n", tokens)
            payload = cache.load_by_hash(key)

            self.assertEqual(payload["tokenizer_fingerprint"], "fingerprint")
            self.assertEqual(payload["tokens"], tokens)

    def test_content_hash_normalizes_line_endings_and_nfkc(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache = TokenCache(Path(tmp), "fingerprint")

            key_a = cache.put("１\r\n２", [])
            key_b = cache.put("1\n2", [])

            self.assertEqual(key_a, key_b)

    def test_mtime_index_round_trips_after_flush(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "input.txt"
            source.write_text("猫", encoding="utf-8")
            mtime = source.stat().st_mtime_ns

            cache = TokenCache(root / "cache", "fingerprint")
            cache.put_by_mtime(source, mtime, "猫", [{"surface": "猫"}])
            cache.flush_mtime_index()

            reloaded = TokenCache(root / "cache", "fingerprint")
            key = reloaded.get_hash_by_mtime(source, mtime)

            self.assertIsNotNone(key)
            self.assertEqual(reloaded.load_by_hash(key)["tokens"], [{"surface": "猫"}])

    def test_load_by_hash_removes_bad_pickle_and_returns_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache = TokenCache(Path(tmp), "fingerprint")
            bad_key = "a" * 64
            bad_path = Path(tmp) / f"{bad_key}.pkl"
            bad_path.write_text("not pickle", encoding="utf-8")

            self.assertIsNone(cache.load_by_hash(bad_key))
            self.assertFalse(bad_path.exists())


if __name__ == "__main__":
    unittest.main()
