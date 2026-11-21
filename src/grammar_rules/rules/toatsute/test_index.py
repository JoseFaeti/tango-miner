import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.toatsute.index import RULE, match_toatsute


class ToatsuteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'人気の店とあって、行列ができた。': 'とあって',
                 '休日とあって混んでいる。': 'とあって',
                 '友達と会って話した。': None,
                 '休日とあっては困る。': None,
                 '休日とあっても困る。': None,
                 '休日と あって混んだ。': None,
                 'とあって混んだ。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toatsute(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('休日とあって混んだ。', [
            tok('休日', '休日', ('名詞',)), tok('と', 'と', ('助詞',)),
            tok('あっ', 'ある', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('混ん', '混む', ('動詞',)), tok('だ', 'た', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['と', 'あっ', 'て'])
        words = {'休日': object()}
        stage.add_cards(words)
        self.assertIn('休日', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'とあって')
        self.assertIn(RULE, RULES)
