import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_choudai.index import RULE, match_te_choudai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.registry import RULES


class TeChoudaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '食べてちょうだい。': 'てちょうだい', '読んで頂戴。': 'で頂戴',
            '食べてちょうだいね。': 'てちょうだい', '食べないでちょうだい。': 'ないでちょうだい',
            '本をちょうだい。': None, '雨でちょうだい。': None,
            '食べて 頂戴。': None, '食べて頂戴する。': None,
            '食べて頂戴した。': None, '高くないでちょうだい。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_te_choudai(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_card(self):
        source = sentence('これを食べてちょうだい。', [
            tok('これを', 'これ', ('名詞',)),
            tok('食べ', '食べる', ('動詞', '一般', '*', '*', '下一段', '連用形-一般')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('ちょうだい', 'ちょうだい', ('名詞', '普通名詞', 'サ変可能')),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=RULES)
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source, source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['て', 'ちょうだい'])
        words = {'食べる': object()}
        stage.add_cards(words)
        self.assertIn('食べる', words)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'てちょうだい')
        self.assertEqual(len(words[RULE.expression].sentences), 1)
