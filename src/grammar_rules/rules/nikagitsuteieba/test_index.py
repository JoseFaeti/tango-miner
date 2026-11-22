import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nikagitsuteieba.index import RULE, match_nikagitsuteieba
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NikagitsuteiebaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '今年に限って言えば、売上は増えた。': 'に限って言えば',
            '彼にかぎっていえば、問題はない。': 'にかぎっていえば',
            '今年に限って言うと、売上は増えた。': None,
            '今年に限って言えない。': None,
            '今年に限って雨が降る。': None,
            '今年に限って 言えば、増えた。': None,
            '時間を限って言えばいい。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nikagitsuteieba(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_overlap_and_identity(self):
        source = sentence('今年に限って言えば売上は増えた。', [
            tok('今年', '今年', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('限っ', '限る', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('言え', '言う', ('動詞',)), tok('ば', 'ば', ('助詞', '接続助詞')),
            tok('売上は増えた。', '売上', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '限っ', 'て', '言え', 'ば'])
        words = {'今年': object()}
        stage.add_cards(words)
        self.assertIn('今年', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に限って言えば')
        self.assertEqual(words['〜に限って'].sentences[0].surface_form, 'に限って')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
