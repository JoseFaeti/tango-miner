import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nitsuke.index import RULE as SINGLE
from src.grammar_rules.rules.nitsukenitsuke.index import RULE as PAIRED
from src.grammar_rules.rules.nitsuketeha.index import RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class NitsuketehaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            'その件につけては後で話す。': ['につけては'],
            '見るにつけては故郷を思う。': ['につけては'],
            '雨につけては風につけては心配だ。': ['につけては', 'につけては'],
            '雨につけ故郷を思う。': [],
            '何事につけても思う。': [],
            '彼につけては心配ない。': ['につけては'],
            '壁に着けては外す。': [],
            'その件につけて、はっきり話す。': [],
            '壁に付けては外す。': [],
            '壁につけて使う。': [],
            '雨に つけても思う。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in RULE.match(tokens)], expected)
                if expected:
                    self.assertEqual(list(SINGLE.match(tokens)), [])
                if 'につけては風につけては' in text:
                    self.assertEqual(list(PAIRED.match(tokens)), [])

    def test_reservation_and_identity(self):
        text = '雨につけては故郷を思い出す。'
        raw = [tok('雨', '雨', ('名詞',)), tok('に', 'に', ('助詞', '格助詞')),
               tok('つけ', 'つける', ('動詞',)), tok('て', 'て', ('助詞',)),
               tok('は', 'は', ('助詞',)), tok('故郷', '故郷', ('名詞',)),
               tok('を', 'を', ('助詞',)), tok('思い出す', '思い出す', ('動詞',)),
               tok('。', '。', ('補助記号',))]
        stage = GrammarStep()
        artifact = stage.process(Artifact([sentence(text, raw)]))
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertFalse(artifact.data[0].tokens[0].get('grammar_reserved'))
        self.assertTrue(all(t.get('grammar_reserved') for t in artifact.data[0].tokens[1:5]))
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'につけては')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)

    def test_combined_tokens_and_gaps(self):
        for marker in ([tok('につけ', 'につけ', ('助詞',))],
                       [tok('に', 'に', ('助詞', '格助詞')), tok('つけ', 'つける', ('動詞',))]):
            for ending in ([tok('ては', 'ては', ('助詞',))],
                           [tok('て', 'て', ('助詞',)), tok('は', 'は', ('助詞',))]):
                raw = [tok('件', '件', ('名詞',))] + marker + ending
                text = ''.join(t['surface'] for t in raw)
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in RULE.match(tokens)], ['につけては'])
                for i in range(1, len(raw)):
                    gap_text = ''.join(t['surface'] for t in raw[:i]) + ' ' + ''.join(t['surface'] for t in raw[i:])
                    tokens, error = aligned_tokens(gap_text, raw)
                    self.assertIsNone(error)
                    self.assertEqual(list(RULE.match(tokens)), [])
