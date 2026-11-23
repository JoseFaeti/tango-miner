import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.tohaienai.index import RULE, match_tohaienai
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class TohaienaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '良いとは言えない。': 'とは言えない',
            '十分とはいえません。': 'とはいえません',
            '安全とは言えなかった。': 'とは言えなかった',
            'そうとは言えなくなる。': 'とは言えなく',
            '彼には言えない。': None,
            '安いとはいえ、高い。': None,
            'そうとは言える。': None,
            'そうと は言えない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tohaienai(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('この結果は良いとは言えないですね。', [
            tok('この結果は', '結果', ('名詞',)),
            tok('良い', '良い', ('形容詞',)), tok('と', 'と', ('助詞', '格助詞')),
            tok('は', 'は', ('助詞', '係助詞')),
            tok('言え', '言える', ('動詞',)), tok('ない', 'ない', ('助動詞',)),
            tok('ですね。', 'です', ('助動詞',))])
        stage = GrammarStep()
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['と', 'は', '言え', 'ない'])
        words = {'良い': object()}
        stage.add_cards(words)
        self.assertIn('良い', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'とは言えない')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
