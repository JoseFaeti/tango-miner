import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.Artifact import Artifact
from src.ProcessingStep import ProcessingStep
from src.TokenizeDirectoryStep import (
    TokenizeDirectoryStep,
    count_files,
    count_token_occurrences,
    iter_input_files,
)

from tests.helpers import make_stats


class TokenizeDirectoryStepTests(unittest.TestCase):
    def build_tree(self):
        tmp = tempfile.TemporaryDirectory()
        root = Path(tmp.name)
        (root / "a.txt").write_text("a", encoding="utf-8")
        (root / "b.md").write_text("ignored", encoding="utf-8")
        nested = root / "nested"
        nested.mkdir()
        (nested / "c.srt").write_text("c", encoding="utf-8")
        return tmp, root

    def test_iter_input_files_filters_extensions_and_respects_recursion(self):
        tmp, root = self.build_tree()
        self.addCleanup(tmp.cleanup)

        non_recursive = [path.name for _, path in iter_input_files(root, include_subdirectories=False)]
        recursive = [path.name for _, path in iter_input_files(root, include_subdirectories=True)]

        self.assertEqual(non_recursive, ["a.txt"])
        self.assertEqual(recursive, ["a.txt", "c.srt"])
        self.assertEqual(count_files(root, include_subdirectories=True), 2)

    def test_process_combines_tokens_and_sentences_from_each_file(self):
        tmp, root = self.build_tree()
        self.addCleanup(tmp.cleanup)
        events = []

        def fake_tokenize(path, word_data, segmented_sentences, cache_dir=None, progress_handler=None):
            word = Path(path).stem
            word_data[word] = make_stats(lemma=word)
            segmented_sentences.append(f"sentence-{word}")
            if progress_handler:
                progress_handler(ProcessingStep.TOKENIZING, 1, 2)
            return word_data, segmented_sentences

        step = TokenizeDirectoryStep(root, include_subdirectories=True)
        step._progress_handler = lambda event: events.append(event)

        with patch("src.TokenizeDirectoryStep.appdirs.user_cache_dir", return_value=str(root / "cache")):
            with patch("src.TokenizeDirectoryStep.tokenize", side_effect=fake_tokenize):
                artifact = step.process(Artifact(root))

        self.assertEqual(set(artifact.data.keys()), {"a", "c"})
        self.assertEqual(artifact.sentences, ["sentence-a", "sentence-c"])
        self.assertEqual(events[-1].step, ProcessingStep.TOKENIZING)
        self.assertIn("2 files", events[-1].message)

    def test_count_token_occurrences_uses_frequencies_not_unique_lemmas(self):
        words = {
            "a": make_stats(frequency=2),
            "b": make_stats(frequency=5),
        }

        self.assertEqual(count_token_occurrences(words), 7)

    def test_process_reports_total_token_occurrences(self):
        tmp, root = self.build_tree()
        self.addCleanup(tmp.cleanup)
        events = []

        def fake_tokenize(path, word_data, segmented_sentences, cache_dir=None, progress_handler=None):
            word = Path(path).stem
            frequency = 2 if word == "a" else 3
            word_data[word] = make_stats(lemma=word, frequency=frequency)
            return word_data, segmented_sentences

        step = TokenizeDirectoryStep(root, include_subdirectories=True)
        step._progress_handler = lambda event: events.append(event)

        with patch("src.TokenizeDirectoryStep.appdirs.user_cache_dir", return_value=str(root / "cache")):
            with patch("src.TokenizeDirectoryStep.tokenize", side_effect=fake_tokenize):
                step.process(Artifact(root))

        self.assertIn("5 tokens", events[-1].message)


if __name__ == "__main__":
    unittest.main()
