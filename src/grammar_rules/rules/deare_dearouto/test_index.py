import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.deare_dearouto.index import RULE, match_deare_dearouto
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class DeareDearoutoTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'誰であれ許さない。': ['であれ'], '学生であろうと許さない。': ['であろうと'],
                 '雨であれ雪であれ行く。': ['であれ', 'であれ'],
                 '学生であれば行ける。': [], '学生であろう。': [],
                 '学校で歩く。': [], '学生で ある。': []}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_deare_dearouto(tokens)], expected, raw)

    def test_reservation_identity_and_overlap(self):
        source = sentence('学生であろうと行く。', [tok('学生', '学生', ('名詞',)),
            tok('で', 'だ', ('助動詞',)), tok('あろう', 'ある', ('動詞',)),
            tok('と', 'と', ('助詞',)), tok('行く', '行く', ('動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        artifact = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens if t.get('grammar_reserved')],
                         ['で', 'あろう', 'と'])
        words = {'学生': object()}
        stage.add_cards(words)
        self.assertIn('学生', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'であろうと')
        self.assertIn('〜であろう', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
