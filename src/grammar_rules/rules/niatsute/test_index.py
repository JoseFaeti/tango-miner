import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.niatsute.index import RULE, match_niatsute
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NiatsuteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'戦時下にあって人々は苦しんだ。': 'にあって',
                 '困難な状況にあっても希望を失わない。': 'にあって',
                 '会議にあっては発言する。': 'にあって',
                 '危機にあっての決断。': 'にあって',
                 '状況に有っても大丈夫。': 'に有って',
                 '友達に会って話す。': None, '状況にある。': None,
                 '状況に あっても大丈夫。': None, 'あっても大丈夫。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_niatsute(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('危機にあって希望を持つ。', [
            tok('危機', '危機', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('あっ', 'ある', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('希望を持つ。', '希望', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'あっ', 'て'])
        words = {'危機': object()}
        stage.add_cards(words)
        self.assertIn('危機', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'にあって')
        self.assertIn('ある / いる', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
