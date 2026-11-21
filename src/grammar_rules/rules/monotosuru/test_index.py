import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.monotosuru.index import RULE, match_monotosuru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MonotosuruTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '費用は本人が負担するものとする。': 'ものとする',
            '既に支払ったものとした。': 'ものとした',
            '安全なものとします。': 'ものとします',
            '本物であるものとしました。': 'ものとしました',
            '買ったものとする。': 'ものとする',  # Also a literal thing reading.
            '私のものとする。': None,
            '来る物とする。': None,
            '来る ものとする。': None,
            '来るものと する。': None,
            '来るものとして扱う。': None,
            '来るものとしない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_monotosuru(tokens)],
                                 [] if expected is None else [expected])

    def test_components_and_identity(self):
        source = sentence('明日は来るものとする。', [
            tok('明日は', '明日', ('名詞',)),
            tok('来る', '来る', ('動詞', '*', '*', '*', '*', '連体形-一般')),
            tok('もの', 'もの', ('名詞',)), tok('と', 'と', ('助詞',)),
            tok('する', 'する', ('動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertFalse(any(t.get('grammar_reserved') for t in data.data[0].tokens))
        words = {'来る': object(), 'もの': object()}
        stage.add_cards(words)
        self.assertIn('来る', words)
        self.assertIn('もの', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ものとする')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
