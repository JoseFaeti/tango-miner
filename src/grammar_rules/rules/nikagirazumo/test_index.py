import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nikagirazumo.index import RULE, match_nikagirazumo
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NikagirazumoTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '子供に限らず大人も楽しめる。': 'に限らず',
            '子供にかぎらず大人も楽しめる。': 'にかぎらず',
            'これにかぎらず他にもある。': None,
            '日本に限らず世界も変わる。': 'に限らず',
            '日本に限らず世界で人気だ。': None,
            '子供に限って大人も楽しめる。': None,
            '子供に限らず、大人も楽しめる。': None,
            '子供に限らず大人。彼も楽しめる。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nikagirazumo(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_overlap(self):
        source = sentence('子供に限らず大人も楽しめる。', [
            tok('子供', '子供', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('限ら', '限る', ('動詞',)), tok('ず', 'ず', ('助動詞',)),
            tok('大人', '大人', ('名詞',)), tok('も', 'も', ('助詞',)),
            tok('楽しめる。', '楽しむ', ('動詞',))])
        stage = GrammarStep()
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '限ら', 'ず'])
        words = {'子供': object(), '大人': object()}
        stage.add_cards(words)
        self.assertIn('子供', words)
        self.assertIn('大人', words)
        self.assertIn('〜に限らず', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に限らず')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
