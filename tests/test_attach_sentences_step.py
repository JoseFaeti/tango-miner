import unittest
from pathlib import Path

from src.AttachSentencesStep import (
    MAX_SENTENCES,
    _compute_sentence_stats,
    _over_level_penalty,
    _percentile,
    _sentence_dedupe_key,
    attach_sentences,
)
from src.SegmentedSentence import SegmentedSentence
from src.WordStats import Sentence

from tests.helpers import make_stats


def seg(text, lemma_surfaces, tag="tag"):
    return SegmentedSentence(
        text=text,
        tag=tag,
        origin=Path("source.txt"),
        lemma_surfaces=lemma_surfaces,
    )


class AttachSentencesStepTests(unittest.TestCase):
    def test_compute_sentence_stats_counts_unknowns_and_variance(self):
        word_data = {
            "低": make_stats(score=100),
            "高": make_stats(score=300),
        }

        scores, unknown, mean, variance = _compute_sentence_stats(
            seg("低と高と謎", {"低": "低", "高": "高", "謎": "謎"}),
            word_data,
        )

        self.assertEqual(scores, [100, 300])
        self.assertEqual(unknown, 1)
        self.assertEqual(mean, 200)
        self.assertEqual(variance, 10000)

    def test_percentile_uses_sorted_values_and_caps_index(self):
        self.assertEqual(_percentile([10, 30, 20], 0.9), 30)
        self.assertEqual(_percentile([10, 30, 20], 0.0), 10)

    def test_over_level_penalty_is_quadratic_for_scores_above_target(self):
        self.assertEqual(_over_level_penalty([100, 150, 250], 150), 10000)

    def test_attach_sentences_keeps_best_three_candidates_per_word(self):
        target = make_stats(score=500, lemma="目標")
        easy = make_stats(score=100, lemma="簡単")
        hard = make_stats(score=3000, lemma="難解")
        word_data = {
            "目標": target,
            "簡単": easy,
            "難解": hard,
        }
        sentences = [
            seg("目標と簡単の例文一", {"目標": "目標", "簡単": "簡単"}, tag="best-1"),
            seg("目標と簡単の例文二", {"目標": "目標", "簡単": "簡単"}, tag="best-2"),
            seg("目標だけの例文三", {"目標": "目標"}, tag="best-3"),
            seg("目標と難解の例文四", {"目標": "目標", "難解": "難解"}, tag="too-hard"),
        ]

        attach_sentences(word_data, sentences)

        self.assertEqual(len(target.sentences), MAX_SENTENCES)
        self.assertNotIn("too-hard", {s.tag for s in target.sentences})
        self.assertTrue(all(isinstance(s, Sentence) for s in target.sentences))

    def test_attach_sentences_keeps_sentence_text_unique_per_word(self):
        target = make_stats(score=500, lemma="目標")
        helper = make_stats(score=100, lemma="簡単")
        word_data = {
            "目標": target,
            "簡単": helper,
        }
        sentences = [
            seg("目標と簡単の同じ例文", {"目標": "目標", "簡単": "簡単"}, tag="first"),
            seg("目標と簡単の同じ例文", {"目標": "目標", "簡単": "簡単"}, tag="duplicate"),
            seg("目標だけの別例文", {"目標": "目標"}, tag="other"),
        ]

        attach_sentences(word_data, sentences)

        texts = [s.text for s in target.sentences]
        self.assertEqual(len(texts), len(set(texts)))
        self.assertEqual(texts.count("目標と簡単の同じ例文"), 1)
        self.assertEqual(len(target.sentences), 2)

    def test_sentence_dedupe_key_ignores_non_japanese_script_prefixes(self):
        self.assertEqual(
            _sentence_dedupe_key("F4　AD　01　01　10　01　01　01>美鶴は切なげに微笑んだ。"),
            "美鶴は切なげに微笑んだ。",
        )
        self.assertEqual(
            _sentence_dedupe_key("F4　AD　01　01　06　01　01　01>美鶴は切なげに微笑んだ。"),
            "美鶴は切なげに微笑んだ。",
        )

    def test_attach_sentences_dedupes_script_prefix_variants_per_word(self):
        target = make_stats(score=500, lemma="美鶴")
        helper = make_stats(score=100, lemma="微笑む")
        word_data = {
            "美鶴": target,
            "微笑む": helper,
        }
        sentences = [
            seg("F4　AD　01　01　10　01　01　01>美鶴は切なげに微笑んだ。", {"美鶴": "美鶴", "微笑む": "微笑ん"}),
            seg("F4　AD　01　01　06　01　01　01>美鶴は切なげに微笑んだ。", {"美鶴": "美鶴", "微笑む": "微笑ん"}),
            seg("美鶴は静かにうなずいた。", {"美鶴": "美鶴"}),
        ]

        attach_sentences(word_data, sentences)

        keys = [_sentence_dedupe_key(s.text) for s in target.sentences]
        self.assertEqual(len(keys), len(set(keys)))
        self.assertEqual(keys.count("美鶴は切なげに微笑んだ。"), 1)


if __name__ == "__main__":
    unittest.main()
