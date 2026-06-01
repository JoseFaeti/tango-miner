import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.Artifact import Artifact
from src.TokenCache import TokenCache
from src.TokenizeStep import TokenizeStep, tokenize


class TokenizeStepTests(unittest.TestCase):
    def test_process_passes_tmpdir_cache_to_tokenize(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            calls = []

            def fake_tokenize(*args, **kwargs):
                calls.append((args, kwargs))
                return {"word": object()}, ["sentence"]

            with patch("src.TokenizeStep.tokenize", side_effect=fake_tokenize):
                artifact = TokenizeStep().process(Artifact("input.txt", tmpdir=tmpdir))

        self.assertEqual(artifact.data.keys(), {"word"})
        self.assertEqual(artifact.sentences, ["sentence"])
        self.assertEqual(calls[0][1]["cache_dir"], tmpdir / "token_cache")

    def test_tokenize_retokens_when_mtime_cache_payload_is_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "input.txt"
            source.write_text("言葉", encoding="utf-8")
            cache_dir = root / "cache"
            mtime = source.stat().st_mtime_ns

            cache = TokenCache(cache_dir, "sudachidict_full+user_dict.C+postproc-v1.2026/06/01.1")
            cache.put_by_mtime(source, mtime, "言葉", [{"surface": "stale"}])
            cache.flush_mtime_index()
            key = cache.get_hash_by_mtime(source, mtime)
            (cache_dir / f"{key}.pkl").unlink()

            class FakeDictionary:
                def __init__(self, *args, **kwargs):
                    pass

                def create(self):
                    return self

                def tokenize(self, text, mode):
                    return [object()]

            replacement_token = {
                "surface": "言葉",
                "lemma": "言葉",
                "base_form": "言葉",
                "reading": "ことば",
                "pos": ("名詞", "普通名詞", "一般", "*"),
            }

            with patch("src.TokenizeStep.dictionary.Dictionary", FakeDictionary):
                with patch("src.TokenizeStep.sudachi_node_to_dict", return_value=replacement_token):
                    word_data, _sentences = tokenize(source, cache_dir=cache_dir)

        self.assertIn("言葉", word_data)
        self.assertEqual(word_data["言葉"].frequency, 1)


if __name__ == "__main__":
    unittest.main()
