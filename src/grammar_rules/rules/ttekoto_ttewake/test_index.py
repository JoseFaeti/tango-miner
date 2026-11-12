import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.tte.index import RULE as SHORT
from src.grammar_rules.rules.ttekoto_ttewake.index import RULE, match_ttekoto_ttewake


class TtekotoTtewakeTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'彼が来るってことだ。': 'ってことだ', '雨だったってわけです。': 'ってわけです',
                 '彼が来るって事だった。': 'って事だった', '彼が来るって訳でした。': 'って訳でした',
                 '彼が来るってこと？': 'ってこと', '彼が来るってわけ。': 'ってわけ',
                 '彼ってことだ。': None, '彼が来るってことになる。': None,
                 '彼が来るってことはない。': None, '彼が来るって ことだ。': None,
                 '彼が来るということだ。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ttekoto_ttewake(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('彼が来るってことだ。', [
            tok('彼', '彼', ('名詞',)), tok('が', 'が', ('助詞',)),
            tok('来る', '来る', ('動詞', '一般', '*', '*', 'カ行変格', '終止形-一般')),
            tok('って', 'って', ('助詞',)), tok('こと', 'こと', ('名詞', '普通名詞')),
            tok('だ', 'だ', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, SHORT))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['って', 'こと', 'だ'])
        self.assertEqual(stage.occurrences[SHORT.id].count, 0)
        words = {'来る': object()}
        stage.add_cards(words)
        self.assertIn('来る', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ってことだ')
        self.assertIn(RULE, RULES)
