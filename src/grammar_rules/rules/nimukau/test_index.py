import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nimukau.index import RULE, match_nimukau
from src.grammar_rules.rules.nimukatte_nimukete.index import match_nimukatte_nimukete
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NimukauTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'駅に向かう。': 'に向かう', '駅に向かった。': 'に向かった',
                 'それに向かう。': 'に向かう', '駅にむかった。': 'にむかった',
                 '駅に向かって歩く。': None, '駅へ向かう。': None,
                 '駅に 向かう。': None, '仕事に向いている。': None,
                 '駅に向ける。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nimukau(tokens)],
                                 [] if expected is None else [expected])
                if '向かって' in text:
                    self.assertEqual([text[a:b] for a, b in match_nimukatte_nimukete(tokens)],
                                     ['に向かって'])

    def test_lexical_retention_and_identity(self):
        source = sentence('私は急いで駅に向かった。', [
            tok('私', '私', ('代名詞',)), tok('は', 'は', ('助詞',)),
            tok('急いで', '急ぐ', ('動詞',)),
            tok('駅', '駅', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('向かっ', '向かう', ('動詞',)), tok('た', 'た', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertFalse(any(t.get('grammar_reserved') for t in data.data[0].tokens))
        words = {'駅': object()}
        stage.add_cards(words)
        self.assertIn('駅', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に向かった')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)

    def test_lexical_retention_with_other_reserved_grammar(self):
        source = sentence('私は駅にむかうと思います。', [
            tok('私', '私', ('代名詞',)), tok('は', 'は', ('助詞',)),
            tok('駅', '駅', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('むかう', 'むかう', ('動詞',)), tok('と', 'と', ('助詞',)),
            tok('思い', '思う', ('動詞',)), tok('ます', 'ます', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        other = GrammarRule(id='test_particle', expression='test particle', reading='',
                            meaning='', match=lambda tokens: [(tokens[5]['span'][0],
                                                               tokens[5]['span'][1])])
        stage = GrammarStep(rules=(RULE, other))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        verb = next(t for t in data.data[0].tokens if t['surface'] == 'むかう')
        self.assertFalse(verb.get('grammar_reserved', False))
        self.assertTrue(any(t.get('grammar_reserved') for t in data.data[0].tokens))
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'にむかう')
