import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nitaisuru.index import RULE, match_nitaisuru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NitaisuruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        from sudachipy import dictionary, tokenizer
        analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        cases = {
            '先生に対する敬意。': 'に対する',
            'これに対する答え。': 'に対する',
            '働くのに対する報酬。': 'に対する',
            '先生にたいする敬意。': 'にたいする',
            '敵に対する者。': 'に対する',
            '問題に対して答える。': None,
            '問題に対しまして答える。': None,
            '問題に対し、答える。': None,
            '敵に対した態度。': None,
            '敵に対さない態度。': None,
            '敵に対します。': None,
            '敵に対する。': None,
            '敵と対する。': None,
            '先生に 対する敬意。': None,
            '先生 に対する敬意。': None,
            '先生に対する 敬意。': None,
            '先生に対する、敬意。': None,
            '対する敬意。': None,
            '走るに対する評価。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nitaisuru(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_examples_and_identity(self):
        source = sentence('先生に対する敬意を忘れない。', [
            tok('先生', '先生', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('対する', '対する', ('動詞',)), tok('敬意', '敬意', ('名詞',)),
            tok('を忘れない。', '忘れる', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        tokens = data.data[0].tokens
        self.assertEqual([t['surface'] for t in tokens if t.get('grammar_reserved')],
                         ['に', '対する'])
        self.assertEqual([t['surface'] for t in tokens
                          if t['surface'] in ('先生', '敬意') and not t.get('grammar_reserved')],
                         ['先生', '敬意'])
        words = {'先生': object(), '敬意': object()}
        stage.add_cards(words)
        self.assertIn('先生', words)
        self.assertIn('敬意', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に対する')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'],
                         RULE.expression)
