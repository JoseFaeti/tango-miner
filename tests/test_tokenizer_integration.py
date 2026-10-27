"""Small end-to-end expectations for the production tokenizer configuration."""

import unittest

from src.steps.TokenizeStep import (
    TOKENIZER_MODE,
    combine_tokens_jmdict,
    get_jmdict,
    get_tokenizer,
    sudachi_node_to_dict,
)


def tokenize_sentence(text: str) -> list[dict]:
    """Parse text with the configured production Sudachi tokenizer."""
    return [
        sudachi_node_to_dict(node)
        for node in get_tokenizer().tokenize(text, TOKENIZER_MODE)
    ]


def token_details(tokens: list[dict]) -> list[tuple[str, str, str, tuple[str, str]]]:
    """Keep the stable fields that define the tokenization contract."""
    return [
        (token["surface"], token["lemma"], token["reading"], tuple(token["pos"][:2]))
        for token in tokens
    ]


class TokenizerIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # These tests deliberately exercise the real Sudachi and JMdict data.
        cls.jmdict = get_jmdict()

    def test_sudachi_tokenizes_a_sentence_with_expected_metadata(self):
        tokens = tokenize_sentence("彼はすでに家に帰っている。")

        self.assertEqual(
            token_details(tokens),
            [
                ("彼", "彼", "かれ", ("代名詞", "*")),
                ("は", "は", "は", ("助詞", "係助詞")),
                ("すでに", "すでに", "すでに", ("副詞", "*")),
                ("家", "家", "いえ", ("名詞", "普通名詞")),
                ("に", "に", "に", ("助詞", "格助詞")),
                ("帰っ", "帰る", "かえる", ("動詞", "一般")),
                ("て", "て", "て", ("助詞", "接続助詞")),
                ("いる", "いる", "いる", ("動詞", "非自立可能")),
                ("。", "。", "。", ("補助記号", "句点")),
            ],
        )

    def test_merges_a_kana_only_word_split_by_sudachi(self):
        raw_tokens = tokenize_sentence("あまりにも静か。")
        merged_tokens = combine_tokens_jmdict(raw_tokens, self.jmdict)

        self.assertEqual([token["surface"] for token in raw_tokens], ["あまり", "にも", "静か", "。"])
        self.assertEqual(
            token_details(merged_tokens),
            [
                ("あまりにも", "あまりにも", "あまりにも", ("exp", "*")),
                ("静か", "静か", "しずか", ("形状詞", "一般")),
                ("。", "。", "。", ("補助記号", "句点")),
            ],
        )

    def test_merges_a_kana_only_noun_after_a_prefix(self):
        """Keep lexical kana nouns such as おまもり, not just まもり."""
        raw_tokens = tokenize_sentence("おまもりをください。")
        merged_tokens = combine_tokens_jmdict(raw_tokens, self.jmdict)

        self.assertEqual(
            [token["surface"] for token in raw_tokens],
            ["お", "まもり", "を", "ください", "。"],
        )
        self.assertEqual(
            [token["surface"] for token in merged_tokens],
            ["おまもり", "を", "ください", "。"],
        )
        self.assertEqual(merged_tokens[0]["lemma"], "おまもり")
        self.assertEqual(merged_tokens[0]["reading"], "おまもり")

    def test_merges_a_mixed_script_compound_without_touching_grammar(self):
        raw_tokens = tokenize_sentence("機械じかけの扉。")
        merged_tokens = combine_tokens_jmdict(raw_tokens, self.jmdict)

        self.assertEqual([token["surface"] for token in raw_tokens], ["機械", "じかけ", "の", "扉", "。"])
        self.assertEqual(
            token_details(merged_tokens),
            [
                ("機械じかけ", "機械じかけ", "きかいじかけ", ("exp", "*")),
                ("の", "の", "の", ("助詞", "格助詞")),
                ("扉", "扉", "とびら", ("名詞", "普通名詞")),
                ("。", "。", "。", ("補助記号", "句点")),
            ],
        )

    def test_preserves_the_source_surface_when_merging_inflected_tokens(self):
        raw_tokens = tokenize_sentence("おまちください。")
        merged_tokens = combine_tokens_jmdict(raw_tokens, self.jmdict)

        self.assertEqual(merged_tokens[0]["surface"], "おまち")
        self.assertEqual(merged_tokens[0]["lemma"], "おまち")
        self.assertNotIn("おまつ", [token["lemma"] for token in merged_tokens])

    def test_recovers_split_nouns_from_suspended_example_sentences(self):
        cases = [
            ("さあ、さいだんのマナの種子に聖剣をかかげてみなさい。", "さいだん", "さい"),
            ("ラライのガケさいだんの形と像の形が合わないようだ。", "さいだん", "さい"),
            ("言っのはんていは王様アーと王女様それと私コック長バの３人によるたすうけつで行いますペタペ", "はんてい", "はん"),
            ("まさかメディばあさんが賢者の末裔だったとはおどろきでがす。", "ばあさん", "ばあ"),
            ("ローラばあさんはむかし酒場の歌ひめをやっとったんじゃ。", "ばあさん", "ばあ"),
            ("私のことを助けが必要なおばあちゃんと思ったかい？", "ばあちゃん", "ばあ"),
            ("言っには、おにい様あだけでなくみなさんの命もかかっていますの！", "みなさん", "みな"),
            ("なんだかあのおっさんにいいように使われてるでがすなあ。", "おっさん", "おっ"),
            ("悪いやつらには、ないしょだよおじいちゃんは、お城の東にある、神殿にいるのさ。", "じいちゃん", "じい"),
            ("じょっじょうちゃん…ぼうは、ぼうはいかん！", "じょうちゃん", "じょう"),
            ("しかし…じょうちゃんはめつきが悪いぞそんなんじゃ男にもてんぞNAME:03", "じょうちゃん", "じょう"),
        ]
        for sentence, expected, fragment in cases:
            with self.subTest(sentence=sentence):
                merged = combine_tokens_jmdict(tokenize_sentence(sentence), self.jmdict)
                self.assertIn(expected, [token["lemma"] for token in merged])
                self.assertNotIn(fragment, [token["lemma"] for token in merged])
                token = next(t for t in merged if t["lemma"] == expected)
                self.assertEqual("".join(c.surface for c in token["components"]), expected)

    def test_neighboring_grammar_and_standalone_words_remain(self):
        cases = [
            ("そんなことしてみろ。", "こと", "ことし"),
            ("できないし困る。", "ない", "ないし"),
            ("ないしは別の方法だ。", "ないし", None),
            ("ことしは良い年になる。", "ことし", None),
            ("行かないでください。", "ないで", None),
            ("おまちください。", "おまち", "おまつ"),
        ]
        for sentence, expected, forbidden in cases:
            with self.subTest(sentence=sentence):
                merged = combine_tokens_jmdict(tokenize_sentence(sentence), self.jmdict)
                lemmas = [token["lemma"] for token in merged]
                self.assertIn(expected, lemmas)
                if forbidden:
                    self.assertNotIn(forbidden, lemmas)

    def test_does_not_turn_negative_grammar_into_the_unrelated_word_nushi(self):
        raw_tokens = tokenize_sentence("少なくとも歓迎できる事態ではありませんし。")
        merged_tokens = combine_tokens_jmdict(raw_tokens, self.jmdict)

        self.assertNotIn("ぬし", [token["lemma"] for token in merged_tokens])


if __name__ == "__main__":
    unittest.main()
