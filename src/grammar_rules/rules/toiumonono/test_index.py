import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.toiu.index import match_toiu
from src.grammar_rules.rules.toiumonono.index import RULE, match_toiumonono


class ToiumononoTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '春というものの、まだ寒い。': 'というものの',
            '便利と言うものの、難しい。': 'と言うものの',
            '買ったというものの、使わない。': 'というものの',
            '春というものの名前。': None,
            '春というものの。': None,
            '春というものの、。寒い。': None,
            '春というもののまだ寒い。': None,
            '春とはいうものの、寒い。': None,
            '春と いうものの、寒い。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toiumonono(tokens)],
                                 [] if expected is None else [expected])
                if expected:
                    self.assertEqual(list(match_toiu(tokens)), [])

    def test_reservation_and_identity(self):
        source = sentence('春というものの、寒い。', [
            tok('春', '春', ('名詞',)), tok('と', 'と', ('助詞', '格助詞')),
            tok('いう', 'いう', ('動詞',)), tok('もの', 'もの', ('名詞',)),
            tok('の', 'の', ('助詞',)), tok('、', '、', ('補助記号',)),
            tok('寒い。', '寒い', ('形容詞',))])
        stage = GrammarStep()
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['と', 'いう', 'もの', 'の'])
        words = {'春': object()}
        stage.add_cards(words)
        self.assertIn('春', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'というものの')
        self.assertEqual(stage.occurrences['toiu'].count, 0)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
