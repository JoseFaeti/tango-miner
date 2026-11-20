import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.tomonaruto.index import RULE, match_tomonaruto
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TomonarutoTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '夏ともなると暑い。': 'ともなると',
            'プロともなると違う。': 'ともなると',
            '大学に入るともなると忙しい。': 'ともなると',
            '友となると嬉しい。': None,
            '夏ともなれば暑い。': None,
            '夏ともなる。': None,
            '夏とも なると暑い。': None,
            'ともなると暑い。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tomonaruto(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_card_identity(self):
        source = sentence('夏ともなると毎日とても暑い。', [
            tok('夏', '夏', ('名詞',)), tok('と', 'と', ('助詞',)),
            tok('も', 'も', ('助詞',)), tok('なる', 'なる', ('動詞',)),
            tok('と', 'と', ('助詞',)), tok('毎日とても暑い。', '暑い', ('形容詞',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['と', 'も', 'なる', 'と'])
        self.assertFalse(result.data[0].tokens[0].get('grammar_reserved'))
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ともなると')


if __name__ == '__main__':
    unittest.main()
