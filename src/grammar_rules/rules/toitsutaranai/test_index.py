import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.toitsutaranai.index import RULE, match_toitsutaranai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class ToitsutaranaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '嬉しいといったらない。': 'といったらない',
            '怖いと言ったらない。': 'と言ったらない',
            '暑いといったらなかった。': 'といったらなかった',
            '寂しいったらない。': 'ったらない',
            '彼と言ったら何もない。': None,
            '嬉しいといったらある。': None,
            '嬉しいと いったらない。': None,
            '嬉しいといったらなかっ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toitsutaranai(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('嬉しいといったらない。', [
            tok('嬉しい', '嬉しい', ('形容詞',)), tok('と', 'と', ('助詞',)),
            tok('いっ', 'いう', ('動詞',)), tok('たら', 'た', ('助動詞',)),
            tok('ない', 'ない', ('形容詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['と', 'いっ', 'たら', 'ない'])
        words = {'嬉しい': object()}
        stage.add_cards(words)
        self.assertIn('嬉しい', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'といったらない')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
