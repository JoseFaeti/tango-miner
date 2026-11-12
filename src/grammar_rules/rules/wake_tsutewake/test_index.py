import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.wake_tsutewake.index import RULE, match_wake


class WakeTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'雨が降ったわけ。': 'わけ', '寒い訳？': '訳', '静かなわけ。': 'わけ',
                 '行くわけ': 'わけ', '行くわけだ。': None, '行くわけがない。': None,
                 '行くわけを聞いた。': None, 'そのわけ。': None,
                 '行くってわけ。': None, '行く わけ。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_wake(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('私は明日そこへ行くわけ。', [
            tok('私は明日そこへ', '私', ('名詞',)),
            tok('行く', '行く', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('わけ', 'わけ', ('名詞', '普通名詞')), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')], ['わけ'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'わけ')
        self.assertIn('行く', words)
        self.assertIn(RULE, RULES)
