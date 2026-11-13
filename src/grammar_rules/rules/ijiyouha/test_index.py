import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.ijiyouha.index import RULE, match_ijiyouha
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class IjiyouhaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '引き受けた以上は最後まで頑張る。': '以上は',
            '行く以上楽しもう。': '以上',
            '学生である以上は勉強する。': '以上は',
            '静かな以上は安心だ。': '以上は',
            '高い以上は品質がいいはずだ。': '以上は',
            '行かない以上は仕方ない。': '以上は',
            '十人以上は来る。': None,
            'それ以上は無理だ。': None,
            '以上は終わりです。': None,
            '行く 以上は頑張る。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ijiyouha(tokens)],
                                 [] if expected is None else [expected])

    def test_card_and_reservation(self):
        source = sentence('行く以上は最後まで頑張る。', [
            tok('行く', '行く', ('動詞', '一般', '*', '*', '五段-カ行', '連体形-一般')),
            tok('以上', '以上', ('名詞', '普通名詞')),
            tok('は', 'は', ('助詞', '係助詞')),
            tok('最後まで', '最後', ('名詞',)),
            tok('頑張る。', '頑張る', ('動詞',))])
        stage = GrammarStep(rules=(RULE,))
        artifact = stage.process(Artifact([source]))
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, '以上は')
        self.assertIn('行く', words)
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens
                          if t.get('grammar_reserved')], ['以上', 'は'])
