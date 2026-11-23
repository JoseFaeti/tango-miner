import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.naikotoniha.index import RULE as SHORT_RULE
from src.grammar_rules.rules.naikotonihahajimaranai.index import (
    RULE,
    match_naikotonihahajimaranai,
)
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NaikotonihahajimaranaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            'やってみないことには始まらない。': 'ないことには始まらない',
            'お金がないことには始まらない。': 'ないことには始まらない',
            '行かないことにははじまらなかった。': 'ないことにははじまらなかった',
            '行かないことには始まりません。': 'ないことには始まりません',
            '行かないことには始まりませんでした。': 'ないことには始まりませんでした',
            '行かないことには始まる。': None,
            '行かないことには分からない。': None,
            '行くことには始まらない。': None,
            '行かないことには 始まらない。': None,
            '行かないことには始まらなかっ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_naikotonihahajimaranai(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_overlap(self):
        parts = [tok('行か', '行く', ('動詞',)), tok('ない', 'ない', ('助動詞',)),
                 tok('こと', 'こと', ('名詞',)), tok('に', 'に', ('助詞',)),
                 tok('は', 'は', ('助詞',)), tok('始まら', '始まる', ('動詞',)),
                 tok('ない', 'ない', ('助動詞',)), tok('。', '。', ('補助記号',))]
        stage = GrammarStep(rules=(RULE, SHORT_RULE))
        result = ExcludeGrammarTokensStep(stage).process(
            stage.process(Artifact([sentence('行かないことには始まらない。', parts)])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ない', 'こと', 'に', 'は', '始まら', 'ない'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ないことには始まらない')
        self.assertIn(SHORT_RULE.expression, words)
        from src.grammar_rules.registry import RULES
        from src.GrammarConstants import GRAMMAR_EXPRESSIONS
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertIn(RULE, RULES)
