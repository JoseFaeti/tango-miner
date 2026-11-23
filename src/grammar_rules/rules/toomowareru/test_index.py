import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.monotoomowareru.index import RULE as MONO_RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.toomowareru.index import RULE, match_toomowareru
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class ToomowareruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '事故が原因だと思われる。': 'と思われる',
            '被害が拡大したと思われた。': 'と思われた',
            '今後増えると思われます。': 'と思われます',
            '彼が犯人と思われました。': 'と思われました',
            '事故が原因であるものと思われる。': None,
            '彼が来ると 思われる。': None,
            '彼が来ると思う。': None,
            '彼が来ると思われない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toomowareru(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('彼は明日来ると思われる。', [
            tok('彼は明日', '彼', ('名詞',)),
            tok('来る', '来る', ('動詞', '*', '*', '*', '*', '終止形-一般')),
            tok('と', 'と', ('助詞', '格助詞')), tok('思わ', '思う', ('動詞',)),
            tok('れる', 'れる', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, MONO_RULE))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['と', '思わ', 'れる'])
        words = {'来る': object()}
        stage.add_cards(words)
        self.assertIn('来る', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'と思われる')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)

    def test_mono_rule_owns_its_longer_form(self):
        source = sentence('来るものと思われる。', [
            tok('来る', '来る', ('動詞', '*', '*', '*', '*', '連体形-一般')),
            tok('もの', 'もの', ('名詞',)), tok('と', 'と', ('助詞', '格助詞')),
            tok('思わ', '思う', ('動詞',)), tok('れる', 'れる', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, MONO_RULE))
        stage.process(Artifact([source]))
        words = {}
        stage.add_cards(words)
        self.assertIn(MONO_RULE.expression, words)
        self.assertNotIn(RULE.expression, words)
