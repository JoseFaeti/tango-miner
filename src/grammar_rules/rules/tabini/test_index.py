import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import TABINI_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.tabini.index import match_tabini
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TabiniTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '会うたびに笑う。': 'たびに', '行く度に思い出す。': '度に',
            '旅行のたびに写真を撮る。': 'たびに', '旅行の度に写真を撮る。': '度に',
            '旅に出る。': None, '足袋に穴が開いた。': None,
            'たびに出会う。': None, '会ったたびに笑う。': None,
            '会うたびは笑う。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tabini(aligned)],
                                 [] if expected is None else [expected])

    def test_card_preserves_attached_word_and_evidence(self):
        source = sentence('旅行のたびに写真を撮る。', [
            tok('旅行', '旅行', ('名詞',)), tok('の', 'の', ('助詞', '格助詞')),
            tok('たび', 'たび', ('名詞',)), tok('に', 'に', ('助詞', '格助詞')),
            tok('写真を撮る。', '撮る', ('動詞',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['たび', 'に'])
        words = {'旅行': object()}
        stage.add_cards(words)
        self.assertIn('旅行', words)
        self.assertEqual(words[TABINI_EXPRESSION].frequency, 2)
        self.assertEqual(words[TABINI_EXPRESSION].sentences[0].surface_form, 'たびに')
