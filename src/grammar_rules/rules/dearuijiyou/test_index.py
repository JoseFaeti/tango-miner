import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.dearuijiyou.index import RULE, match_dearuijiyou
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class DearuijiyouTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '学生である以上は勉強する。': 'である以上は',
            '静かである以上安心だ。': 'である以上',
            '行く以上は頑張る。': None,
            '学生であった以上は頑張る。': None,
            '学生である 以上は頑張る。': None,
            '十人以上は来る。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_dearuijiyou(tokens)],
                                 [] if expected is None else [expected])

    def test_identity_overlap_and_reservation(self):
        source = sentence('学生である以上は勉強する。', [
            tok('学生', '学生', ('名詞',)), tok('で', 'だ', ('助動詞',)),
            tok('ある', 'ある', ('動詞', '非自立可能', '*', '*', '五段-ラ行', '連体形-一般')),
            tok('以上', '以上', ('名詞', '普通名詞')), tok('は', 'は', ('助詞', '係助詞')),
            tok('勉強する。', '勉強', ('名詞',))])
        stage = GrammarStep(prefer_expressions=False)
        artifact = stage.process(Artifact([source]))
        words = {'学生': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'である以上は')
        self.assertIn('〜である', words)
        self.assertIn('〜以上は', words)
        self.assertIn('学生', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens
                          if t.get('grammar_reserved')], ['で', 'ある', '以上', 'は'])
