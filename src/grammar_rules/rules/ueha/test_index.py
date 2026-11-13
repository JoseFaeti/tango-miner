import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.ueha.index import RULE, match_ueha
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class UehaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '引き受けた上は最後まで頑張る。': '上は',
            '決めたうえはやり遂げる。': 'うえは',
            'こうなる上は仕方がない。': '上は',
            '机の上はきれいだ。': None,
            '上は青空だ。': None,
            '決めた 上は頑張る。': None,
            '決めた上では頑張る。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ueha(tokens)],
                                 [] if expected is None else [expected])

    def test_card_and_reservation(self):
        source = sentence('決めた上は最後まで頑張る。', [
            tok('決め', '決める', ('動詞', '一般', '*', '*', '下一段', '連用形-一般')),
            tok('た', 'た', ('助動詞',)), tok('上', '上', ('名詞', '普通名詞')),
            tok('は', 'は', ('助詞', '係助詞')), tok('最後まで', '最後', ('名詞',)),
            tok('頑張る。', '頑張る', ('動詞',))])
        stage = GrammarStep(rules=(RULE,))
        artifact = stage.process(Artifact([source]))
        words = {'決める': object()}
        stage.add_cards(words)
        self.assertIn('決める', words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, '上は')
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens
                          if t.get('grammar_reserved')], ['上', 'は'])
