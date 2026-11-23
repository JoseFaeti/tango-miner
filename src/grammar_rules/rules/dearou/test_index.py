import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.dearou.index import RULE, match_dearou
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class DearouTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'彼は学生であろう。': True, '静かであろう。': True,
                 '学生であろう人。': True, '行くのであろう。': True,
                 '学校であろう会議。': True, '学生である。': False,
                 '学生だろう。': False, '学校で歩こう。': False,
                 '学生で あろう。': False}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_dearou(tokens)],
                                 ['であろう'] if expected else [])

    def test_boundaries_and_parts_of_speech(self):
        de = tok('で', 'だ', ('助動詞',))
        arou = tok('あろう', 'ある', ('動詞',))
        cases = [('であろう', [de, arou]),
                 ('学生であろう', [tok('学生', '学生', ('名詞',)), tok('で', 'で', ('助詞',)), arou]),
                 ('学生であろう', [tok('学生', '学生', ('名詞',)), de, tok('あろう', 'ある', ('名詞',))])]
        for text, raw in cases:
            with self.subTest(text=text, raw=raw):
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual(list(match_dearou(tokens)), [])

    def test_reservation_and_identity(self):
        source = sentence('彼は優秀な学生であろう。', [tok('彼は優秀な', '彼', ('名詞',)),
            tok('学生', '学生', ('名詞',)), tok('で', 'だ', ('助動詞',)),
            tok('あろう', 'ある', ('動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        artifact = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens if t.get('grammar_reserved')],
                         ['で', 'あろう'])
        words = {'学生': object()}
        stage.add_cards(words)
        self.assertIn('学生', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'であろう')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
