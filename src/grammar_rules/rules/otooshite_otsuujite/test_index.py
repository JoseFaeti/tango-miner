import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.otooshite_otsuujite.index import RULE, match_otooshite_otsuujite
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class OtooshiteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '友人を通して知った。': 'を通して', '一年を通して暖かい。': 'を通して',
            '経験を通じて学ぶ。': 'を通じて', '一年を通じて営業する。': 'を通じて',
            '友人を通しての連絡。': 'を通して', '道を通る。': None,
            '話が通じて安心した。': None, '糸を通した。': None,
            '友人を 通して知った。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_otooshite_otsuujite(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('友人を通じて知らせを受け取った。', [
            tok('友人', '友人', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('通じ', '通じる', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('知らせを受け取った。', '知らせ', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '通じ', 'て'])
        words = {'友人': object()}
        stage.add_cards(words)
        self.assertIn('友人', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を通じて')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
