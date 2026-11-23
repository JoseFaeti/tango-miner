import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.monotoshite.index import RULE, match_monotoshite
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.toshite.index import RULE as TOSHITE_RULE
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class MonotoshiteTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼は来るものとして計画する。': True,
            '費用は支払ったものとして扱う。': True,
            '安全なものとして考える。': True,
            '本物であるものとして扱う。': True,
            '買ったものとして売る。': True,  # Also a literal thing reading.
            '私のものとして使う。': False,
            '教師として働く。': False,
            '来る物として扱う。': False,
            '来る ものとして扱う。': False,
            '来るものと して扱う。': False,
            '来るものとする。': False,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_monotoshite(tokens)],
                                 ['ものとして'] if expected else [])

    def test_overlap_components_and_identity(self):
        source = sentence('来るものとして計画する', [
            tok('来る', '来る', ('動詞', '*', '*', '*', '*', '連体形-一般')),
            tok('もの', 'もの', ('名詞',)), tok('と', 'と', ('助詞',)),
            tok('し', 'する', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('計画する', '計画する', ('動詞',))])
        single = GrammarStep(rules=(RULE,))
        single_data = ExcludeGrammarTokensStep(single).process(single.process(Artifact([source])))
        self.assertFalse(any(t.get('grammar_reserved') for t in single_data.data[0].tokens))
        stage = GrammarStep(rules=(RULE, TOSHITE_RULE))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['と', 'し', 'て'])
        words = {'来る': object(), 'もの': object()}
        stage.add_cards(words)
        self.assertIn('来る', words)
        self.assertIn('もの', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ものとして')
        self.assertIn(TOSHITE_RULE.expression, words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
