import unittest
from pathlib import Path

from src.SegmentedSentence import SegmentedSentence
from src.steps.AttachSentencesStep import (
    MAX_SENTENCES,
    _percentile,
    _sentence_dedupe_key_fast,
    _sentence_penalty_fast,
    _sentence_quality_penalty_fast,
    attach_sentences,
)

from tests.helpers import make_stats


def seg(text, lemma_surfaces, tag="tag"):
    return SegmentedSentence(
        text=text,
        tag=tag,
        origin=Path("source.txt"),
        lemma_surfaces=lemma_surfaces,
    )


class AttachSentencesStepTests(unittest.TestCase):
    def test_percentile_uses_sorted_values_and_caps_index(self):
        self.assertEqual(_percentile([10, 30, 20], 0.9), 30)
        self.assertEqual(_percentile([10, 30, 20], 0.0), 10)

    def test_sentence_penalties_prefer_clean_complete_text(self):
        clean = _sentence_quality_penalty_fast("美鶴は切なげに微笑んだ。")
        edited = _sentence_quality_penalty_fast("美鶴は[...]に微笑んだ。")
        incomplete = _sentence_quality_penalty_fast("美鶴は切なげに微笑んだ")

        self.assertLess(clean, edited)
        self.assertLess(clean, incomplete)
        self.assertGreater(_sentence_penalty_fast("短い。", [100], 1, 0), 0)

    def test_dedupe_key_removes_script_prefix_only_when_body_is_japanese(self):
        self.assertEqual(
            _sentence_dedupe_key_fast("F4 AD 01>美鶴は切なげに微笑んだ。"),
            "美鶴は切なげに微笑んだ。",
        )
        self.assertEqual(_sentence_dedupe_key_fast("plain > text"), "plain > text")

    def test_attach_sentences_keeps_unique_candidates_up_to_the_limit(self):
        target = make_stats(score=500, lemma="目標")
        helper = make_stats(score=100, lemma="簡単")
        word_data = {"目標": target, "簡単": helper}
        sentences = [
            seg("目標と簡単の例文一。", {"目標": "目標", "簡単": "簡単"}, "one"),
            seg("目標と簡単の例文二。", {"目標": "目標", "簡単": "簡単"}, "two"),
            seg("目標と簡単の例文三。", {"目標": "目標", "簡単": "簡単"}, "three"),
            seg("目標と簡単の例文一。", {"目標": "目標", "簡単": "簡単"}, "duplicate"),
        ]

        attach_sentences(word_data, sentences)

        self.assertEqual(len(target.sentences), MAX_SENTENCES)
        texts = [sentence.text for sentence in target.sentences]
        self.assertEqual(len(texts), len(set(texts)))


if __name__ == "__main__":
    unittest.main()
