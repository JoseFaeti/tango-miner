import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.kagirinioite.index import RULE, match_kagirinioite
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KagirinioiteTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '私が知る限りにおいては正しい。': '限りにおいて',
            '確認した限りにおいて問題はない。': '限りにおいて',
            '可能な限りにおいて協力する。': '限りにおいて',
            '私の記憶の限りにおいて正しい。': '限りにおいて',
            '知るかぎりにおいては正しい。': 'かぎりにおいて',
            '会議において発表する。': None,
            '知る限りにおける問題。': None,
            '知る 限りにおいて正しい。': None,
            '知る限りに おいて正しい。': None,
            '限りにおいて正しい。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kagirinioite(tokens)],
                                 [] if expected is None else [expected], tokens)

    def test_reservation_and_identity(self):
        source = sentence('知る限りにおいて正しい。', [
            tok('知る', '知る', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('限り', '限り', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('おい', 'おく', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('正しい。', '正しい', ('形容詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['限り', 'に', 'おい', 'て'])
        words = {'知る': object()}
        stage.add_cards(words)
        self.assertIn('知る', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, '限りにおいて')
        self.assertIn('〜において / 〜における', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
