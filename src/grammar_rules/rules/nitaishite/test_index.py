import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nitaishite.index import RULE, match_nitaishite
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NitaishiteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'先生に対して失礼だ。': 'に対して',
                 '兄に対して弟は静かだ。': 'に対して',
                 'これに対しては反対だ。': 'に対して',
                 '問題に対しまして答える。': 'に対しまして',
                 '問題に対する答え。': None, '敵と対して戦う。': None,
                 '問題に 対して答える。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nitaishite(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('先生に対して丁寧に話しましょう。', [
            tok('先生', '先生', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('対し', '対する', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('丁寧に話しましょう。', '丁寧', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '対し', 'て'])
        words = {'先生': object()}
        stage.add_cards(words)
        self.assertIn('先生', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に対して')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
