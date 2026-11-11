import unittest
from pathlib import Path

from src.TokenizedSentence import TokenizedSentence
from src.steps.BuildWordStatsStep.index import build_word_stats


class EligibilityCacheTest(unittest.TestCase):
    def test_same_lemma_keeps_pos_rule_and_reservation_distinct(self):
        def token(pos, **extra):
            return {"surface": "風花", "lemma": "風花", "base_form": "風花",
                    "reading": "フウカ", "pos": pos, **extra}

        tokens = [token(("名詞", "固有名詞")),
                  token(("名詞", "固有名詞"), analysis_rule="rise_name_boundary"),
                  token(("名詞", "普通名詞"), grammar_reserved=True),
                  token(("名詞", "普通名詞"))]
        words, sentences = build_word_stats([
            TokenizedSentence("風花風花風花風花", "tag", Path("source"), tokens)
        ])
        self.assertEqual(words["風花"].frequency, 2)
        self.assertEqual(len(sentences[0].lemma_occurrences["風花"]), 2)


if __name__ == "__main__":
    unittest.main()
