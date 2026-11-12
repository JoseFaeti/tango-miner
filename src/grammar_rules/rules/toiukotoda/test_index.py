import unittest
from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.toiukotoda.index import RULE, match_toiukotoda
from src.grammar_rules.rules.toiu.index import RULE as SHORT
from src.grammar_rules.registry import RULES


class ToiukotodaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'明日は雨だということだ。': 'ということだ', '彼が来るということです。': 'ということです',
                 '彼が来たということだった。': 'ということだった', '彼が来ると言う事でした。': 'と言う事でした',
                 '彼が来るっていうことだ。': 'っていうことだ', '彼が来るということはない。': None,
                 '彼が来るということになる。': None, '猫ということだ。': None, '彼が来ると いうことだ。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toiukotoda(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('明日彼が行くということだ。', [
            tok('明日', '明日', ('名詞',)), tok('彼', '彼', ('名詞',)), tok('が', 'が', ('助詞',)),
            tok('行く', '行く', ('動詞', '一般', '*', '*', '五段-カ行', '終止形-一般')),
            tok('と', 'と', ('助詞',)), tok('いう', '言う', ('動詞',)),
            tok('こと', 'こと', ('名詞', '普通名詞')), tok('だ', 'だ', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, SHORT))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['と', 'いう', 'こと', 'だ'])
        self.assertEqual(stage.occurrences[SHORT.id].count, 0)
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ということだ')
        self.assertIn(RULE, RULES)
