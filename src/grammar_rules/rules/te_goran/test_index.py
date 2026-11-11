import unittest

from src.Grammar import GrammarStep
from src.Artifact import Artifact
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_goran.index import RULE, match_te_goran
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TeGoranTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'食べてごらん。': 'てごらん', '読んでごらん。': 'でごらん',
                 '見てご覧なさい。': 'てご覧なさい', '開けて御覧。': 'て御覧',
                 '読んでごらんください。': 'でごらんください',
                 'ご覧になる。': None, '歩いてご覧になる。': None,
                 'ご覧ください。': None, '雨でごらん。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_te_goran(aligned)],
                                 [] if expected is None else [expected])

    def test_card_keeps_verb_and_identity(self):
        source = sentence('この料理を食べてごらん。', [
            tok('この料理を', '料理', ('名詞',)),
            tok('食べ', '食べる', ('動詞', '一般', '*', '*', '下一段', '連用形-一般')),
            tok('て', 'て', ('助詞', '接続助詞')), tok('ごらん', 'ごらん', ('名詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        stage.process(Artifact([source]))
        words = {'食べる': object()}
        stage.add_cards(words)
        self.assertIn('食べる', words)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'てごらん')
