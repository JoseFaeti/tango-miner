import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.kanogotoku.index import RULE, match_kanogotoku
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KanogotokuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼は全て知っているかのごとく話す。': 'かのごとく',
            '夢を見たかの如く笑った。': 'かの如く',
            '嬉しいかのごとく振る舞う。': 'かのごとく',
            '知っているかのごとき顔だ。': None,
            '鹿のごとく走る。': None,
            '知っているかのように話す。': None,
            '知っているかの ごとく話す。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kanogotoku(aligned)],
                                 [expected] if expected else [])

    def test_reservation_example_and_registry(self):
        source = sentence('知っているかのごとく話す。', [
            tok('知っている', 'いる', ('動詞', '一般', '*', '*', '*', '連体形-一般')),
            tok('か', 'か', ('助詞', '副助詞')),
            tok('の', 'の', ('助詞', '格助詞')),
            tok('ごとく', 'ごとし', ('助動詞',)),
            tok('話す', '話す', ('動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=RULES)
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['か', 'の', 'ごとく'])
        self.assertFalse(result.data[0].tokens[0].get('grammar_reserved'))
        words = {'知る': object(), '話す': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'かのごとく')
        self.assertIn('知る', words)
        self.assertIn('話す', words)
