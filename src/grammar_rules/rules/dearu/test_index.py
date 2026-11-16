import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.dearu.index import RULE, match_dearu
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class DearuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'彼は学生である。': 'である', '静かである。': 'である',
                 '学生であった。': 'であった', '学生であれば行ける。': 'であれば',
                 '学生である人。': 'である', '行くのである。': 'である',
                 '学校である会議。': 'である', '学校で歩く。': None,
                 '机の上である。': 'である', '学生で ある。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_dearu(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('彼は優秀な学生である。', [tok('彼は優秀な', '彼', ('名詞',)),
            tok('学生', '学生', ('名詞',)), tok('で', 'だ', ('助動詞',)),
            tok('ある', 'ある', ('動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        artifact = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens if t.get('grammar_reserved')],
                         ['で', 'ある'])
        words = {'学生': object()}
        stage.add_cards(words)
        self.assertIn('学生', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'である')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)

    def test_explanatory_copula_overlap(self):
        source = sentence('彼は学校へ行くのである。', [tok('彼は学校へ', '彼', ('名詞',)),
            tok('行く', '行く', ('動詞',)), tok('の', 'の', ('助詞', '準体助詞')),
            tok('で', 'だ', ('助動詞',)), tok('ある', 'ある', ('動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        stage.process(Artifact([source]))
        words = {}
        stage.add_cards(words)
        self.assertIn('〜んだ', words)
        self.assertIn(RULE.expression, words)
