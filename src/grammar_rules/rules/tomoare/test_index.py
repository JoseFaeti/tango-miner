import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.tomoare.index import RULE, match_tomoare
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class TomoareTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            'ともあれ、先に進もう。': True,
            '何はともあれ無事でよかった。': True,
            '理由はともあれ、続けよう。': True,
            '友あれば楽しい。': False,
            'ともあれば行く。': False,
            '何であれ構わない。': False,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tomoare(tokens)],
                                 ['ともあれ'] if expected else [])

    def test_reservation_and_identity(self):
        source = sentence('理由はともあれ続けよう。', [
            tok('理由', '理由', ('名詞',)), tok('は', 'は', ('助詞', '係助詞')),
            tok('ともあれ', 'ともあれ', ('副詞',)), tok('続けよう。', '続ける', ('動詞',))])
        stage = GrammarStep()
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['は', 'ともあれ'])
        words = {'理由': object()}
        stage.add_cards(words)
        self.assertIn('理由', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ともあれ')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
