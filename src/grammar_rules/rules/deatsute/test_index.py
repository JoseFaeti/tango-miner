import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.deatsute.index import RULE, match_deatsute
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class DeatsuteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'彼は学生であって、教師でもある。': True,
                 '静かであってほしい。': True, '友人であっても油断できない。': True,
                 '事故であって事件ではない。': True, '机の上であってはならない。': True,
                 '学校で会っている。': False, '学生であった。': False,
                 '学生で  あってほしい。': False}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_deatsute(tokens)],
                                 ['であって'] if expected else [])

    def test_reservation_and_identity(self):
        source = sentence('彼は学生であって、教師だ。', [tok('彼は', '彼', ('名詞',)),
            tok('学生', '学生', ('名詞',)), tok('で', 'だ', ('助動詞',)),
            tok('あっ', 'ある', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('、', '、', ('補助記号',)), tok('教師', '教師', ('名詞',)),
            tok('だ', 'だ', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        artifact = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens if t.get('grammar_reserved')],
                         ['で', 'あっ', 'て', 'だ'])
        words = {'学生': object()}
        stage.add_cards(words)
        self.assertIn('学生', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'であって')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
