import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.denakutenandarou.index import RULE, match_denakutenandarou
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class DenakutenandarouTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            'これが奇跡でなくて何だろう。': 'でなくて何だろう',
            'それが幸せでなくて何だろうか。': 'でなくて何だろう',
            '奇跡じゃなくて何だろう。': None,
            '奇跡でなくて何だ。': None,
            '奇跡でなくて誰だろう。': None,
            '奇跡でなくて 何だろう。': None,
            'これが愛でなくて何だろう。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_denakutenandarou(tokens)],
                                 [] if expected is None else [expected])

    def test_card_and_reservation(self):
        source = sentence('奇跡でなくて何だろう。', [
            tok('奇跡', '奇跡', ('名詞',)), tok('で', 'だ', ('助動詞',)),
            tok('なく', 'ない', ('形容詞',)), tok('て', 'て', ('助詞',)),
            tok('何', '何', ('代名詞',)), tok('だろう', 'だ', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['で', 'なく', 'て', '何', 'だろう'])
        words = {'奇跡': object()}
        stage.add_cards(words)
        self.assertIn('奇跡', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'でなくて何だろう')
