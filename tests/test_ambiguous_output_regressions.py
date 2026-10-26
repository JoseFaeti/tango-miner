"""Recall-first regressions derived from the Anki-output audit.

These examples deliberately cover grammar, fixed expressions, colloquialisms,
and kana-only text. They protect the project's goal of retaining useful
material from the learner's reading corpus while ensuring a merge never
silently changes the displayed source form into an unrelated dictionary word.
"""

import unittest

from src.steps.MergeTokensStep import combine_tokens_jmdict, get_jmdict
from src.steps.TokenizeStep import (
    TOKENIZER_MODE,
    get_tokenizer,
    sudachi_node_to_dict,
    tokenize,
)


def raw_tokens(text: str) -> list[dict]:
    return [
        sudachi_node_to_dict(node)
        for node in get_tokenizer().tokenize(text, TOKENIZER_MODE)
    ]


class AmbiguousOutputRegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.jmdict = get_jmdict()

    def test_keeps_grammar_and_fixed_expressions(self):
        cases = {
            "これが答えなんだ。": "なんだ",
            "これは大事なんです。": "んです",
            "ここでは戦えない。": "では",
            "行ってもいいよ。": "ても",
            "行かないでください。": "ないで",
            "行かないと間に合わない。": "ないと",
            "彼のために戦う。": "ために",
            "一人でもできる。": "一人で",
            "少なくとも歓迎できる事態ではありませんし。": "少なくとも",
        }

        for sentence, expected in cases.items():
            with self.subTest(sentence=sentence, expected=expected):
                words, _ = tokenize([sentence])
                self.assertIn(expected, words)

    def test_keeps_kana_only_and_colloquial_learning_material(self):
        cases = {
            "長年探し続けて、ようやく見つけた。": "ようやく",
            "何をしようが自由だ。": "しよう",
            "さあ、さいだんへ行こう。": "さい",
            "お前の後についてくから適当にがんばってくれ。": "てく",
            "楽しいおもいでができた。": "おもいで",
            "うわ、かっけえ！": "かっけえ",
            "今日はだりぃ。": "だりぃ",
            "それはちげぇよ。": "ちげぇ",
        }

        for sentence, expected in cases.items():
            with self.subTest(sentence=sentence, expected=expected):
                words, _ = tokenize([sentence])
                self.assertIn(expected, words)

    def test_dictionary_merges_keep_the_exact_displayed_source_form(self):
        sentences = [
            "おまもりをください。",
            "おまちください。",
            "これが答えなんだ。",
            "これは大事なんです。",
            "ここでは戦えない。",
            "行かないでください。",
            "彼のために戦う。",
            "うわ、かっけえ！",
            "今日はだりぃ。",
        ]

        for sentence in sentences:
            with self.subTest(sentence=sentence):
                merged = combine_tokens_jmdict(raw_tokens(sentence), self.jmdict)
                dictionary_merges = [token for token in merged if token["pos"][0] == "exp"]
                self.assertTrue(dictionary_merges)
                self.assertTrue(
                    all(token["surface"] == token["lemma"] for token in dictionary_merges)
                )

    def test_known_lemma_inventions_do_not_return(self):
        cases = {
            "おまちください。": "おまつ",
            "少なくとも歓迎できる事態ではありませんし。": "ぬし",
        }

        for sentence, unexpected in cases.items():
            with self.subTest(sentence=sentence, unexpected=unexpected):
                words, _ = tokenize([sentence])
                self.assertNotIn(unexpected, words)


if __name__ == "__main__":
    unittest.main()
