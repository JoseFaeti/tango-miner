import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.dearouto_dearouto.index import RULE, match_dearouto_dearouto
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class DearoutoDearoutoTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨であろうと雪であろうと行く。': ['であろうと', 'であろうと'],
            '学生であろうと、社会人であろうと参加できる。': ['であろうと', 'であろうと'],
            '学生であろうと参加できる。': [],
            '雨であれ雪であろうと行く。': [],
            '雨であろうと雪である。': [],
            '雨であろうと。雪であろうと。': [],
            '学校で歩こうと家で歩こうとする。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_dearouto_dearouto(tokens)], expected)

    def test_reservation_and_identity(self):
        source = sentence('雨であろうと雪であろうと行く。', [
            tok('雨', '雨', ('名詞',)), tok('で', 'だ', ('助動詞',)),
            tok('あろう', 'ある', ('動詞',)), tok('と', 'と', ('助詞',)),
            tok('雪', '雪', ('名詞',)), tok('で', 'だ', ('助動詞',)),
            tok('あろう', 'ある', ('動詞',)), tok('と', 'と', ('助詞',)),
            tok('行く', '行く', ('動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        artifact = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens if t.get('grammar_reserved')],
                         ['で', 'あろう', 'と', 'で', 'あろう', 'と'])
        words = {'雨': object(), '雪': object()}
        stage.add_cards(words)
        self.assertIn('雨', words)
        self.assertIn('雪', words)
        self.assertIn(RULE.expression, words)
        self.assertIn('〜であれ / 〜であろうと', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
