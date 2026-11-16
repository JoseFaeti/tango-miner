import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.oyoginakusuru.index import RULE, match_oyoginakusuru
from src.grammar_rules.rules.oyoginakusareru.index import match_oyoginakusareru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class OyoginakusuruTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '中止を余儀なくする。': 'を余儀なくする',
            '中止を余儀なくした。': 'を余儀なくした',
            '中止を余儀なくしている。': 'を余儀なくして',
            '中止を余儀なくします。': 'を余儀なくします',
            '中止を余儀なくしました。': 'を余儀なくしました',
            '中止を余儀なくしない。': 'を余儀なくしない',
            '中止を余儀なくしなかった。': 'を余儀なくしなかった',
            '中止を余儀なくすれば困る。': 'を余儀なくすれば',
            '中止を余儀なくされる。': None, '余儀なくする。': None,
            '中止を 余儀なくする。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_oyoginakusuru(tokens)],
                                 [] if expected is None else [expected])
                if expected is not None:
                    self.assertEqual(list(match_oyoginakusareru(tokens)), [])

    def test_reservation_and_identity(self):
        source = sentence('中止を余儀なくした。', [
            tok('中止', '中止', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('余儀', '余儀', ('名詞',)), tok('なく', 'ない', ('形容詞',)),
            tok('し', 'する', ('動詞',)), tok('た', 'た', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '余儀', 'なく', 'し', 'た'])
        words = {'中止': object()}
        stage.add_cards(words)
        self.assertIn('中止', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を余儀なくした')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
