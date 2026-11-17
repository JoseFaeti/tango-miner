import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.osashioite.index import RULE, match_osashioite
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class OsashioiteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'先輩を差し置いて彼が選ばれた。': 'を差し置いて',
                 '親をさしおいて遊ぶ。': 'をさしおいて',
                 '彼を差しおいて決めた。': 'を差しおいて',
                 '先輩を差し置きまして決めた。': 'を差し置きまして',
                 '先輩を差し置く。': None,
                 '先輩に差し置いて決めた。': None,
                 '先輩を 差し置いて決めた。': None,
                 '荷物を置いて行く。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_osashioite(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('先輩を差し置いて彼が選ばれた。', [
            tok('先輩', '先輩', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('差し置い', '差し置く', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('彼が選ばれた。', '彼', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '差し置い', 'て'])
        words = {'先輩': object()}
        stage.add_cards(words)
        self.assertIn('先輩', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を差し置いて')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
