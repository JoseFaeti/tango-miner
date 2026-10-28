import tempfile
import unittest
from pathlib import Path
from src.TokenCache import TokenCache


class TokenCacheTests(unittest.TestCase):
    def test_order_and_sentence_boundaries_affect_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache = TokenCache(Path(tmp), "version")
            self.assertNotEqual(cache._compute_key(["a\nb"]), cache._compute_key(["a", "b"]))
            self.assertNotEqual(cache._compute_key(["a", "b"]), cache._compute_key(["b", "a"]))

    def test_roundtrip_and_corruption(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache = TokenCache(Path(tmp), "version")
            tokens = [[{"surface": "猫", "lemma": "猫", "base_form": "猫", "reading": "ねこ", "pos": ("名詞",)}]]
            cache.put(["猫"], tokens)
            self.assertEqual(cache.load(["猫"]), tokens)
            path = cache.cache_dir / (cache._compute_key(["猫"]) + ".pkl")
            path.write_text("bad", encoding="utf-8")
            self.assertIsNone(cache.load(["猫"]))
            cache.put(["猫"], [])
            self.assertIsNone(cache.load(["猫"]))
