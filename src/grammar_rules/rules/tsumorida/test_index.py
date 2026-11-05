import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import TSUMORIDA_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.tsumorida.index import match_tsumorida
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TsumoridaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '明日行くつもりだ。': 'つもりだ',
            '明日行くつもりです。': 'つもりです',
            '明日行くつもりだった。': 'つもりだった',
            '明日行くつもりでした。': 'つもりでした',
            '明日行かないつもりだ。': 'つもりだ',
            '明日行く積もりだ。': '積もりだ',
            '行くつもりはない。': None,
            '行くつもりなら話そう。': None,
            '彼のつもりだ。': None,
            '雪が積もりだした。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tsumorida(aligned)],
                                 [] if expected is None else [expected], raw)

    def test_card_preserves_verb_and_evidence(self):
        source = sentence('私は明日学校へ行くつもりだ。', [
            tok('私は明日学校へ', '学校', ('名詞',)),
            tok('行く', '行く', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('つもり', 'つもり', ('名詞', '普通名詞')),
            tok('だ', 'だ', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['つもり', 'だ'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[TSUMORIDA_EXPRESSION].frequency, 2)
        self.assertEqual(words[TSUMORIDA_EXPRESSION].sentences[0].surface_form, 'つもりだ')
