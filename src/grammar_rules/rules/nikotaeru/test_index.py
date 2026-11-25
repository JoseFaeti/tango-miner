import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nikotaeru.index import RULE, match_nikotaeru
from src.grammar_rules.rules.nikotaete.index import match_nikotaete
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NikotaeruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer

            analyzer = dictionary.Dictionary(
                config_path="sudachi.config.json", dict="full"
            ).create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f"Sudachi full dictionary unavailable: {exc}")
        cases = {
            "期待に応える判断。": "に応える",
            "期待に応えた計画。": "に応えた",
            "期待に応えます。": "に応えます",
            "期待に応えました。": "に応えました",
            "期待に応えない。": "に応えない",
            "期待に応えなかった。": "に応えなかった",
            "期待に応えません。": "に応えません",
            "期待に応えませんでした。": "に応えませんでした",
            "それに応える。": "に応える",
            "要求にこたえた計画。": "にこたえた",
            "要求にこたえます。": "にこたえます",
            "要求にこたえました。": "にこたえました",
            "要求にこたえない。": "にこたえない",
            "要求にこたえなかった。": "にこたえなかった",
            "要求にこたえませんでした。": "にこたえませんでした",
            "要求にこたえれば。": "にこたえれば",
            "要求にこたえよう。": "にこたえよう",
            "期待に応えて判断する。": None,
            "期待に応えさせて判断する。": None,
            "期待に応えられる人。": "に応えられる",
            "期待に応えさせられる。": None,
            "期待に応え られる。": None,
            "応える判断。": None,
            "要求に 応える。": None,
            "質問に答える。": None,
            "期待に応じる。": None,
            "期待を応える。": None,
            "期待に、応える。": None,
            "期待 に応える。": None,
            "暑さが身にこたえる。": "にこたえる",
            "期待に応える人。": "に応える",
            "期待に応えた。": "に応えた",
            "期待に応えるなら頑張ろう。": "に応える",
            "期待に応えれば。": "に応えれば",
            "期待に応えたら。": "に応えたら",
            "期待に応えなければ。": "に応えなければ",
            "期待に応えなかったら。": "に応えなかったら",
            "期待に応えなきゃ。": "に応えなきゃ",
            "期待に応えよう。": "に応えよう",
            "期待に応えましょう。": "に応えましょう",
            "期待に応えられる。": "に応えられる",
            "期待に応えられた。": "に応えられた",
            "期待に応えられれば。": "に応えられれば",
            "期待に応えられます。": "に応えられます",
            "期待に応えられました。": "に応えられました",
            "期待に応えられない。": "に応えられない",
            "期待に応えられなかった。": "に応えられなかった",
            "期待に応えられません。": "に応えられません",
            "期待に応えられませんでした。": "に応えられませんでした",
            "期待に応えさせる。": "に応えさせる",
            "期待に応えさせた。": "に応えさせた",
            "期待に応えさせます。": "に応えさせます",
            "期待に応えさせました。": "に応えさせました",
            "期待に応えさせない。": "に応えさせない",
            "期待に応えさせなかった。": "に応えさせなかった",
            "期待に応えさせません。": "に応えさせません",
            "期待に応えさせませんでした。": "に応えさせませんでした",
            "期待に応えさせれば。": "に応えさせれば",
            "期待に応えさせたら。": "に応えさせたら",
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [
                    tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                    for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)
                ]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual(
                    [text[a:b] for a, b in match_nikotaeru(tokens)],
                    [] if expected is None else [expected],
                )
                if "応えて" in text:
                    self.assertEqual(
                        [text[a:b] for a, b in match_nikotaete(tokens)], ["に応えて"]
                    )

    def test_reservation_and_identity(self):
        source = sentence(
            "期待に応えませんでした。",
            [
                tok("期待", "期待", ("名詞",)),
                tok("に", "に", ("助詞",)),
                tok("応え", "応える", ("動詞",)),
                tok("ませ", "ます", ("助動詞",)),
                tok("ん", "ぬ", ("助動詞",)),
                tok("でし", "です", ("助動詞",)),
                tok("た", "た", ("助動詞",)),
                tok("。", "。", ("補助記号",)),
            ],
        )
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(
            stage.process(Artifact([source]))
        )
        self.assertEqual(
            [t["surface"] for t in data.data[0].tokens if t.get("grammar_reserved")],
            ["に", "応え", "ませ", "ん", "でし", "た"],
        )
        words = {"期待": object()}
        stage.add_cards(words)
        self.assertIn("期待", words)
        self.assertEqual(
            words[RULE.expression].sentences[0].surface_form, "に応えませんでした"
        )
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(
            LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + " (grammar)"], RULE.expression
        )

    def test_extended_auxiliary_reservation(self):
        potential = sentence(
            "期待に応えられませんでした。",
            [
                tok("期待", "期待", ("名詞",)),
                tok("に", "に", ("助詞",)),
                tok("応え", "応える", ("動詞",)),
                tok("られ", "られる", ("助動詞",)),
                tok("ませ", "ます", ("助動詞",)),
                tok("ん", "ぬ", ("助動詞",)),
                tok("でし", "です", ("助動詞",)),
                tok("た", "た", ("助動詞",)),
                tok("。", "。", ("補助記号",)),
            ],
        )
        causative = sentence(
            "期待に応えさせたと聞いた。",
            [
                tok("期待", "期待", ("名詞",)),
                tok("に", "に", ("助詞",)),
                tok("応え", "応える", ("動詞",)),
                tok("させ", "させる", ("助動詞",)),
                tok("た", "た", ("助動詞",)),
                tok("と", "と", ("助詞",)),
                tok("聞い", "聞く", ("動詞",)),
                tok("た", "た", ("助動詞",)),
                tok("。", "。", ("補助記号",)),
            ],
        )
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(
            stage.process(Artifact([potential, causative]))
        )
        expected = [
            ["に", "応え", "られ", "ませ", "ん", "でし", "た"],
            ["に", "応え", "させ", "た"],
        ]
        for item, reserved in zip(data.data, expected):
            self.assertEqual(
                [token["surface"] for token in item.tokens if token.get("grammar_reserved")],
                reserved,
            )
            self.assertFalse(item.tokens[0].get("grammar_reserved"))
        words = {"期待": object()}
        stage.add_cards(words)
        self.assertEqual(
            {item.surface_form for item in words[RULE.expression].sentences},
            {"に応えられませんでした", "に応えさせた"},
        )
