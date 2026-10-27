"""Recall-first regressions derived from the Anki-output audit.

These examples deliberately cover grammar, fixed expressions, colloquialisms,
and kana-only text. They protect the project's goal of retaining useful
material from the learner's reading corpus while ensuring a merge never
silently changes the displayed source form into an unrelated dictionary word.
"""

import unittest

from src.steps.MergeTokensStep import combine_tokens_jmdict, get_jmdict
from src.steps.FilterFrequencyStep import filter_useful_words
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
            "さあ、さいだんへ行こう。": "さいだん",
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

    def test_grammatical_boundaries_do_not_create_unrelated_words(self):
        cases = {
            "できないし困る。": ("ないし", "ない"),
            "そんなことしてみろ、命がいくつあっても足りないぜ。": ("ことし", "こと"),
        }
        for sentence, (unexpected, expected) in cases.items():
            with self.subTest(sentence=sentence):
                words, _ = tokenize([sentence])
                self.assertNotIn(unexpected, words)
                self.assertIn(expected, words)

    def test_actual_conjunction_and_year_remain_available(self):
        for sentence, expected in [("ないしは別の方法だ。", "ないし"),
                                   ("ことしは良い年になる。", "ことし")]:
            with self.subTest(sentence=sentence):
                words, _ = tokenize([sentence])
                self.assertIn(expected, words)

    def test_colloquial_kamoshirenai_is_not_split_into_shireru(self):
        for sentence in ["もしかしたらホントに優勝も夢じゃないかもしれねえな。",
                         "運のちからでラプソーンを楽に倒せるかもしれねえでげすぜ。",
                         "明日にはそこへ行くかもしれねぇ。"]:
            with self.subTest(sentence=sentence):
                words, segments = tokenize([sentence])
                self.assertIn("かもしれない", words)
                self.assertNotIn("しれる", words)
                self.assertNotIn("じゃないか", words)
                self.assertIn(segments[0].lemma_surfaces["かもしれない"], sentence)
                components, = words["かもしれない"].component_counts
                self.assertEqual(''.join(c.surface for c in components), segments[0].lemma_surfaces["かもしれない"])
        for sentence, expected in [("素性のしれない者を信じる。", "しれる"),
                                   ("行くかもしれない。", "かもしれない"),
                                   ("夢じゃないか。", "じゃないか")]:
            with self.subTest(sentence=sentence):
                words, _ = tokenize([sentence])
                self.assertIn(expected, words)

    def test_negative_kamoshirenai_variants_keep_the_source_and_components(self):
        for sentence, surface in [
            ("世界に平和をとりもどせるかもしれぬ。", "かもしれぬ"),
            ("何か良い情報が聞けるかもしれませぬ。", "かもしれませぬ"),
            ("明日にはそこへ行くかもしれん。", "かもしれん"),
        ]:
            with self.subTest(sentence=sentence):
                words, segments = tokenize([sentence])
                self.assertIn("かもしれない", words)
                self.assertNotIn("しれる", words)
                self.assertEqual(segments[0].lemma_surfaces["かもしれない"], surface)
                components, = words["かもしれない"].component_counts
                self.assertEqual(''.join(c.surface for c in components), surface)
        for sentence in ["素性のしれぬ者を信じる。", "何かがしれんとは言えない。"]:
            with self.subTest(sentence=sentence):
                words, _ = tokenize([sentence])
                self.assertIn("しれる", words)

    def test_occurrence_span_tracks_the_token_not_an_earlier_substring(self):
        sentence = "変わった後で、石をわったんだ。"
        words, segments = tokenize([sentence])
        self.assertIn("わる", words)
        start, end = segments[0].lemma_spans["わる"]
        self.assertEqual(sentence[start:end], "わっ")
        self.assertEqual(start, sentence.rindex("わっ"))

    def test_negative_adjective_and_auxiliary_do_not_merge_as_naishi(self):
        cases = [
            "このあたりは他に町もないしここで身体を休めていってね。",
            "あ、私もこれ以上、ミヤとの将来は考えたくないし。",
            "できないし困る。",
        ]
        for sentence in cases:
            with self.subTest(sentence=sentence):
                words, _ = tokenize([sentence])
                self.assertIn("ない", words)
                self.assertNotIn("ないし", words)
        for sentence, expected in [("ないしは別の方法だ。", "ないし"),
                                   ("東京ないし大阪に行く。", "ないし"),
                                   ("行かないでください。", "ないで"),
                                   ("行かないと間に合わない。", "ないと")]:
            with self.subTest(sentence=sentence):
                words, _ = tokenize([sentence])
                self.assertIn(expected, words)

    def test_kotoshi_splits_only_with_following_suru_grammar(self):
        for sentence in ["でも先輩はそんなことしなくても、いっぱい貰えますよね。",
                         "悪いことしても、仕方ねのかなとか思う。",
                         "みんなを守るはずの兵士があんなことしちゃあ",
                         "なんでオレがこんなことしなくちゃならないんだよ！",
                         "そんなことしてみろ、命がいくつあっても足りないぜ。"]:
            with self.subTest(sentence=sentence):
                words, _ = tokenize([sentence])
                self.assertNotIn("ことし", words)
                self.assertIn("こと", words)
                self.assertIn("する", words)
                if any(t["surface"] == "ことし" for t in raw_tokens(sentence)):
                    evidence = [c for seq in words["こと"].component_counts for c in seq]
                    self.assertTrue(any(c.surface == "ことし" for c in evidence))
        for sentence in ["ことしは良い年になる。", "ことしでも来年でもいいよ。"]:
            with self.subTest(sentence=sentence):
                words, _ = tokenize([sentence])
                self.assertIn("ことし", words)

    def test_imperatives_do_not_pollute_potential_forms(self):
        for sentence, base, potential in [
            ("フフフせいぜい祈れ。", "祈る", "祈れる"),
            ("祈れ、今こそ執行の時だ。", "祈る", "祈れる"),
            ("奈々ちゃんが代われって言った。", "代わる", "代われる"),
            ("さぁモンショウのありかを吐け！", "吐く", "吐ける"),
        ]:
            with self.subTest(sentence=sentence):
                words, _ = tokenize([sentence])
                self.assertIn(base, words)
                self.assertNotIn(potential, words)
                components, = words[base].component_counts
                original = next(t for t in raw_tokens(sentence) if t["surface"] == components[0].surface)
                self.assertEqual(components[0].lemma, original["lemma"])
        for sentence, expected in [("あなたが祈れるようになった。", "祈れる"),
                                   ("私は神様に祈れないんだ。", "祈れる"),
                                   ("代われるものなら代わりたいよ！", "代われる"),
                                   ("間違えずに全部踊れたら大丈夫。", "踊れる")]:
            with self.subTest(sentence=sentence):
                words, _ = tokenize([sentence])
                self.assertIn(expected, words)

    def test_reassigned_imperatives_do_not_hide_a_real_low_frequency_potential(self):
        sentences = ["代われるものなら代わりたいよ。", "あなたと代われるなら嬉しい。",
                     "奈々ちゃんが代われって言った。", "あなたが代われって言った。"]
        words, segments = tokenize(sentences, tag="fixture")
        self.assertIn("代われる", words)
        self.assertEqual(words["代われる"].frequency, 2)
        self.assertEqual(words["代われる"].reassigned_occurrences, 2)
        kept = filter_useful_words(words, min_frequency=4, keep_percent=100, min_frequency_per_source=0)
        self.assertIn("代われる", kept)
        self.assertEqual(kept["代われる"].frequency, 2)
        for segment in segments:
            if "代われって" in segment.text:
                self.assertNotIn("代われる", segment.lemma_surfaces)
                self.assertIn("代わる", segment.lemma_surfaces)

    def test_imperatives_do_not_create_a_potential_candidate_by_themselves(self):
        words, _ = tokenize(["祈れ、今こそ執行の時だ。", "祈れ、願いは届く。"], tag="fixture")
        self.assertNotIn("祈れる", words)
        self.assertIn("祈る", words)

    def test_potential_form_fallback_is_source_grounded(self):
        for sentence, word, base in [
            ("彼の子孫たちにこうして会えるなんてすごくうれしいよ。", "会える", "会う"),
            ("果たしてもう一度戦ったとして勝てるかどうか。", "勝てる", "勝つ"),
            ("神を超える力を持った私を倒せるはずがなかろう。", "倒せる", "倒す"),
        ]:
            with self.subTest(word=word):
                words, _ = tokenize([sentence])
                stats = words[word]
                self.assertIsNone(self.jmdict.get_most_common_definition(word))
                result = self.jmdict.get_potential_definition(word, stats.component_counts, get_tokenizer())
                self.assertIsNotNone(result)
                self.assertTrue(result.startswith(f"Potential form of {base}:"))
        words, _ = tokenize(["森の中で猫を見れる。"])
        self.assertIsNone(self.jmdict.get_potential_definition("見れる", {}, get_tokenizer()))

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
