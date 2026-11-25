import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.ooshikitsute.index import RULE, match_ooshikitsute
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class OoshikitsuteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        from sudachipy import dictionary, tokenizer
        analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        cases = {
            '両親の反対を押し切って結婚した。': 'を押し切って',
            '批判を押し切って実行した。': 'を押し切って',
            '抵抗を押し切って進んだ。': 'を押し切って',
            '反発を押し切って進んだ。': 'を押し切って',
            '異論を押し切って進んだ。': 'を押し切って',
            '異議を押し切って進んだ。': 'を押し切って',
            '制止を押し切って進んだ。': 'を押し切って',
            '抵抗をおしきって進んだ。': 'をおしきって',
            '紙を押し切って切断した。': None,
            '相手を押し切って勝った。': None,
            '反対を押して進んだ。': None,
            '反対を押し切る。': None,
            '反対を押し切った。': None,
            '反対を押し切らず戻った。': None,
            '押し切って進んだ。': None,
            '反対に押し切って進んだ。': None,
            '反対 を押し切って進んだ。': None,
            '反対を 押し切って進んだ。': None,
            '反対を押し切っ て進んだ。': None,
            '反対、を押し切って進んだ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ooshikitsute(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('反対を押し切って出席した。', [
            tok('反対', '反対', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('押し切っ', '押し切る', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('出席した。', '出席', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '押し切っ', 'て'])
        self.assertFalse(data.data[0].tokens[0].get('grammar_reserved'))
        words = {'反対': object()}
        stage.add_cards(words)
        self.assertIn('反対', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を押し切って')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
