import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.okanete.index import RULE, match_okanete
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class OkaneteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '散歩を兼ねて買い物に行く。': 'を兼ねて',
            '挨拶を兼ねまして伺います。': 'を兼ねまして',
            '食堂を兼ねている。': 'を兼ねて',
            '仕事を兼ねる。': None, '返事をしかねている。': None,
            '彼はかねてから知っていた。': None,
            '散歩を 兼ねて出かける。': None,
            '散歩を兼ねまし て出かける。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_okanete(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('散歩を兼ねて買い物に行く。', [
            tok('散歩', '散歩', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('兼ね', '兼ねる', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('買い物に行く。', '買い物', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '兼ね', 'て'])
        words = {'散歩': object()}
        stage.add_cards(words)
        self.assertIn('散歩', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を兼ねて')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
