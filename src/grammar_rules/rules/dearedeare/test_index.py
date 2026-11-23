import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.dearedeare.index import RULE, match_dearedeare
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class DearedeareTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨であれ雪であれ行く。': ['であれ', 'であれ'],
            '学生であれ、先生であれ参加できる。': ['であれ', 'であれ'],
            '誰であれ許さない。': [],
            '雨であれば雪であれば行く。': [],
            '雨であれ、雪が降る。学生であれ許す。': [],
            '雨であれ雪であろうと行く。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_dearedeare(tokens)], expected, raw)

    def test_reservation_and_identity(self):
        source = sentence('雨であれ雪であれ行く。', [tok('雨', '雨', ('名詞',)),
            tok('で', 'だ', ('助動詞',)), tok('あれ', 'ある', ('動詞',)),
            tok('雪', '雪', ('名詞',)), tok('で', 'だ', ('助動詞',)),
            tok('あれ', 'ある', ('動詞',)), tok('行く', '行く', ('動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        artifact = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens if t.get('grammar_reserved')],
                         ['で', 'あれ', 'で', 'あれ'])
        words = {'雨': object(), '雪': object()}
        stage.add_cards(words)
        self.assertIn('雨', words)
        self.assertIn('雪', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'であれ')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
