import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.oyoginakusareru.index import RULE, match_oyoginakusareru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class OyoginakusareruTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '中止を余儀なくされる。': 'を余儀なくされる',
            '中止を余儀なくされた。': 'を余儀なくされた',
            '中止を余儀なくされている。': 'を余儀なくされて',
            '中止を余儀なくされます。': 'を余儀なくされます',
            '中止を余儀なくされました。': 'を余儀なくされました',
            '中止を余儀なくされない。': 'を余儀なくされない',
            '中止を余儀なくされなかった。': 'を余儀なくされなかった',
            '中止を余儀なくされれば困る。': 'を余儀なくされれば',
            '中止を余儀なくする。': None, '余儀なくされた。': None,
            '中止を 余儀なくされた。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_oyoginakusareru(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('中止を余儀なくされた。', [
            tok('中止', '中止', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('余儀', '余儀', ('名詞',)), tok('なく', 'ない', ('形容詞',)),
            tok('さ', 'する', ('動詞',)), tok('れ', 'れる', ('助動詞',)),
            tok('た', 'た', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '余儀', 'なく', 'さ', 'れ', 'た'])
        words = {'中止': object()}
        stage.add_cards(words)
        self.assertIn('中止', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を余儀なくされた')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
