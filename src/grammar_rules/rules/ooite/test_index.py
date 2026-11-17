import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.ooite.index import RULE, match_ooite
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class OoiteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼をおいてほかに適任者はいない。': 'をおいて',
            '彼女をおいて他に誰がいる。': 'をおいて',
            '彼を置いて他にいない。': 'を置いて',
            '荷物を置いて行く。': None,
            '机に本をおいてください。': None,
            '彼を差し置いて他の人が選ばれた。': None,
            '彼を おいてほかにいない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ooite(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('彼をおいてほかにいない。', [
            tok('彼', '彼', ('代名詞',)), tok('を', 'を', ('助詞',)),
            tok('おい', 'おく', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('ほか', 'ほか', ('名詞',)), tok('にいない。', 'に', ('助詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', 'おい', 'て'])
        words = {'彼': object(), 'ほか': object()}
        stage.add_cards(words)
        self.assertIn('彼', words)
        self.assertIn('ほか', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'をおいて')
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
