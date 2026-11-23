import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.kagirideha.index import RULE, match_kagirideha
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class KagiridehaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '私が知る限りでは彼は元気だ。': '限りでは',
            '調べた限りでは問題ない。': '限りでは',
            '見ている限りでは大丈夫だ。': '限りでは',
            '私の知るかぎりでは本当だ。': 'かぎりでは',
            '私の記憶の限りでは初めてだ。': '限りでは',
            '嬉しい限りではない。': None,
            '命の限りではない。': None,
            '知る 限りでは分からない。': None,
            '知る限り では分からない。': None,
            '限りでは分からない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kagirideha(tokens)],
                                 [] if expected is None else [expected], tokens)

    def test_reservation_and_identity(self):
        source = sentence('知る限りでは本当だ。', [
            tok('知る', '知る', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('限り', '限り', ('名詞',)), tok('で', 'で', ('助詞',)),
            tok('は', 'は', ('助詞',)), tok('本当だ。', '本当', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['限り', 'で', 'は'])
        words = {'知る': object()}
        stage.add_cards(words)
        self.assertIn('知る', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, '限りでは')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
