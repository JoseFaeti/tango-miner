from pathlib import Path
import tempfile
import unittest

from src.TokenCache import TokenCache
from src.CachePaths import remove_legacy_entries


class CacheRetentionTests(unittest.TestCase):
    def test_token_cache_replaces_changed_content_and_fingerprint_per_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / 'source.txt'
            other = root / 'other.txt'
            token = {'surface': 'a', 'lemma': 'a', 'base_form': 'a', 'reading': 'a', 'pos': ('noun',)}
            cache = TokenCache(root, 'one')
            cache.put(['a'], [[token]], source)
            cache.put(['a'], [[token]], other)
            self.assertIsNone(cache.load(['b'], source))
            cache.put(['b'], [[token]], source)
            self.assertEqual(len(list(cache.cache_dir.glob('*.pkl'))), 2)
            self.assertIsNone(cache.load(['a'], source))
            self.assertEqual(cache.load(['a'], other), [[token]])
            changed = TokenCache(root, 'two')
            self.assertIsNone(changed.load(['b'], source))
            changed.put(['b'], [[token]], source)
            self.assertEqual(len(list(cache.cache_dir.glob('*.pkl'))), 2)

    def test_legacy_cleanup_only_removes_hash_named_entries(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / ('a' * 64 + '.pkl')
            keep = root / 'notes.pkl'
            old.write_bytes(b'old')
            keep.write_bytes(b'keep')
            remove_legacy_entries(root, '.pkl')
            self.assertFalse(old.exists())
            self.assertTrue(keep.exists())
