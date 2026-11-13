import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.kawarini.index import RULE, match_kawarini
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KawariniTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            'バスに乗る代わりに歩いた。': '代わりに',
            '行かないかわりに電話した。': 'かわりに',
            '手伝った代わりに食事をごちそうになった。': '代わりに',
            '安い代わりに狭い。': '代わりに',
            '便利な代わりに高い。': '代わりに',
            '牛乳の代わりに豆乳を使う。': None,
            '代わりに行く。': None, '代わりを探す。': None,
            '歩く 代わりに走る。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kawarini(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('歩く代わりに公園まで走りました。', [
            tok('歩く', '歩く', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('代わり', '代わり', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('公園まで走りました。', '公園', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['代わり', 'に'])
        words = {'歩く': object()}
        stage.add_cards(words)
        self.assertIn('歩く', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, '代わりに')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
