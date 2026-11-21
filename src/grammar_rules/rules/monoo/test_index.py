import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.monoo.index import match_monoo
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MonooTests(unittest.TestCase):
    def test_sudachi_forms_and_nominal_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path="sudachi.config.json", dict="full").create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f"Sudachi full dictionary unavailable: {exc}")
        cases = {
            "言ってくれればよかったものを。": True,
            "早く来れば間に合ったものを、残念だ。": True,
            "行けばいいものを。": True,
            "買ったものを食べた。": False,
            "高いものを買った。": False,
            "静かなものを選ぶ。": False,
            "ものを。": False,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_monoo(aligned)],
                                 ["ものを"] if expected else [])

    def test_card_and_lexical_evidence(self):
        source = sentence("早く行けばいいものを。", [
            tok("早く", "早い", ("形容詞",)),
            tok("行けば", "行く", ("動詞",)),
            tok("いい", "いい", ("形容詞", "非自立可能", "*", "*", "形容詞", "連体形-一般")),
            tok("もの", "もの", ("名詞",)), tok("を", "を", ("助詞", "格助詞")),
            tok("。", "。", ("補助記号",))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {"行く": object(), "もの": object()}
        stage.add_cards(words)
        self.assertIn("行く", words)
        self.assertIn("もの", words)
        self.assertEqual(words["〜ものを"].frequency, 2)
        self.assertEqual(words["〜ものを"].sentences[0].surface_form, "ものを")


if __name__ == "__main__":
    unittest.main()
