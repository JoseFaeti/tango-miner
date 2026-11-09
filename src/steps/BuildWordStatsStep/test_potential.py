import unittest
from pathlib import Path

from src.RunConfig import resolve_config
from src.TokenizedSentence import TokenizedSentence
from src.steps.BuildWordStatsStep.index import PotentialFormResolver, build_word_stats
from src.steps.MergeTokensStep.index import get_jmdict
from src.steps.TokenizeStep.index import get_tokenizer, sudachi_node_to_dict


class PotentialFormsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tokenizer = get_tokenizer()
        cls.resolver = PotentialFormResolver(get_jmdict(), cls.tokenizer)

    def collect(self, texts, resolver):
        sentences = [TokenizedSentence(text, "tag", Path("source.txt"),
                     [sudachi_node_to_dict(n) for n in self.tokenizer.tokenize(text)])
                     for text in texts]
        return build_word_stats(sentences, potential_resolver=resolver)

    def test_merge_keeps_occurrences_and_base_reading(self):
        words, sentences = self.collect(["\u66f8\u304f", "\u66f8\u3051\u305f", "\u66f8\u3051\u308b"], self.resolver)
        self.assertIn("\u66f8\u304f", words)
        self.assertNotIn("\u66f8\u3051\u308b", words)
        self.assertEqual(words["\u66f8\u304f"].frequency, 3)
        self.assertEqual(words["\u66f8\u304f"].reading, "\u304b\u304f")
        self.assertIn("\u66f8\u304f", sentences[1].lemma_occurrences)
        self.assertEqual(sentences[1].lemma_occurrences["\u66f8\u304f"][0][0], "\u66f8\u3051")

    def test_distinct_meaning_stays_and_pure_potential_merges(self):
        words, _ = self.collect(["\u884c\u3051\u308b", "\u898b\u308c\u308b"], self.resolver)
        self.assertIn("\u884c\u3051\u308b", words)
        self.assertIn("\u898b\u308b", words)
        self.assertNotIn("\u898b\u308c\u308b", words)

    def test_disabled_keeps_potential(self):
        words, _ = self.collect(["\u66f8\u304f", "\u66f8\u3051\u308b"], None)
        self.assertIn("\u66f8\u3051\u308b", words)
        self.assertTrue(resolve_config().merge.merge_potential_forms)
        self.assertFalse(resolve_config(overrides={"merge": {"merge_potential_forms": False}}).merge.merge_potential_forms)
