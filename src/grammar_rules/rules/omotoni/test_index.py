import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.omotoni.index import RULE, match_omotoni
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class OmotoniTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'事実をもとに作る。': 'をもとに', '資料を基に作る。': 'を基に',
                 '経験を元にした話。': 'を元に', '資料をもとにして作る。': 'をもとに',
                 '資料をもとにする。': 'をもとに', '本を元に戻す。': 'を元に',
                 '彼のもとに行く。': None, '資料をもとめる。': None,
                 '資料を もとに作る。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_omotoni(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_lexical_candidate(self):
        source = sentence('資料をもとに新しい本を作ります。', [
            tok('資料', '資料', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('もと', 'もと', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('新しい本を作ります。', '作る', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', 'もと', 'に'])
        words = {'資料': object()}
        stage.add_cards(words)
        self.assertIn('資料', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'をもとに')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
