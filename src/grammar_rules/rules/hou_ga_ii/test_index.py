import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.hou_ga_ii.index import RULE, match_hou_ga_ii
from src.grammar_rules.rules.ta_hou_ga_ii.index import match_ta_hou_ga_ii
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class HouGaIiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '早く行く方がいい。': '方がいい', 'ここで食べるほうがよい。': 'ほうがよい',
            '先生に聞かれる方が良い。': '方が良い',
            '行った方がいい。': None, '行かない方がいい。': None,
            '猫の方がいい。': None, '赤い方がいい。': None,
            '行く方が悪い。': None, '行く 方がいい。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_hou_ga_ii(aligned)],
                                 [] if expected is None else [expected])
                if text in ('行った方がいい。', '行かない方がいい。'):
                    self.assertEqual(len(list(match_ta_hou_ga_ii(aligned))), 1)

    def test_reservation_and_identity(self):
        source = sentence('明日は早く病院に行く方がいい。', [
            tok('明日は早く病院に', '病院', ('名詞',)),
            tok('行く', '行く', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('方', '方', ('名詞',)), tok('が', 'が', ('助詞',)),
            tok('いい', '良い', ('形容詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['方', 'が', 'いい'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, '方がいい')
