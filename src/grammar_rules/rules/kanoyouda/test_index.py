import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.kanoyouda.index import RULE, match_kanoyouda
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KanoyoudaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼は全て知っているかのようだ。': 'かのようだ',
            '彼は夢を見ているかのようです。': 'かのようです',
            'まるで本当だったかのようだった。': 'かのようだった',
            '彼は嬉しいかのようでした。': 'かのようでした',
            '彼は知っているかのように話す。': None,
            '彼は知っているかのような顔だ。': None,
            '彼は知っているようだ。': None,
            '彼は知っているかのよう だ。': None,
            '鹿のようだ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kanoyouda(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_example(self):
        source = sentence('彼は知っているかのようだ。', [
            tok('彼は', '彼', ('名詞',)),
            tok('知っている', 'いる', ('動詞', '一般', '*', '*', '*', '連体形-一般')),
            tok('か', 'か', ('助詞', '副助詞')),
            tok('の', 'の', ('助詞', '格助詞')),
            tok('よう', 'よう', ('形状詞', '助動詞語幹')),
            tok('だ', 'だ', ('助動詞', '*', '*', '*', '助動詞-ダ', '終止形-一般')),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['か', 'の', 'よう', 'だ'])
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved'))
        words = {'知る': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'かのようだ')
        self.assertIn('知る', words)
