import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nisakidatte.index import RULE, match_nisakidatte
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NisakidatteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '開会に先立って挨拶する。': 'に先立って',
            '出発に先立っての準備。': 'に先立って',
            'それに先立って行う。': 'に先立って',
            '夫に先立って死んだ。': 'に先立って',
            '開会を先立って行う。': None,
            '開会に 先立って行う。': None,
            '先立って行う。': None,
            '開会に先立つ準備。': None,
            '開会に先立った。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nisakidatte(tokens)],
                                 [] if expected is None else [expected], tokens)

    def test_reservation_and_identity(self):
        source = sentence('開会に先立って挨拶する。', [
            tok('開会', '開会', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('先立っ', '先立つ', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('挨拶する。', '挨拶', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '先立っ', 'て'])
        words = {'開会': object()}
        stage.add_cards(words)
        self.assertIn('開会', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に先立って')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
