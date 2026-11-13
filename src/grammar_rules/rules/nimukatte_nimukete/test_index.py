import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nimukatte_nimukete.index import RULE, match_nimukatte_nimukete
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NimukatteNimuketeTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'駅に向かって歩く。': 'に向かって',
                 '目標に向けて努力する。': 'に向けて',
                 '彼にむかって叫ぶ。': 'にむかって',
                 '明日にむけて準備する。': 'にむけて',
                 '彼に向かっては話せない。': 'に向かって',
                 '駅へ向かって歩く。': None, '顔を向けて話す。': None,
                 '駅に向かう。': None, '駅に 向かって歩く。': None,
                 '駅に向いている。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nimukatte_nimukete(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_source(self):
        source = sentence('駅に向かって歩いています。', [
            tok('駅', '駅', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('向かっ', '向かう', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('歩いています。', '歩く', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '向かっ', 'て'])
        words = {'駅': object()}
        stage.add_cards(words)
        self.assertIn('駅', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に向かって')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
