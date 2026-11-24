import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nitaishi.index import (
    RULE,
    match_nitaishi,
)
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NitaishiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'先生に対し礼を述べる。': 'に対し',
                 '兄は静かなのに対し、弟は賑やかだ。': 'に対し',
                 'これに対し反論する。': 'に対し',
                 '敵に対し、警戒する。': 'に対し', '問題に対し': 'に対し',
                 '先生に対して話す。': None, '先生に対しまして話す。': None,
                 '問題に対する答え。': None, '敵に対した。': None,
                 '敵に対しない。': None, '敵と対し戦う。': None,
                 '対比する。': None, '先生に 対し話す。': None,
                 '先生 に対し話す。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nitaishi(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('この問題に対し詳しく話しましょう。', [
            tok('この', 'この', ('連体詞',)),
            tok('問題', '問題', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('対し', '対する', ('動詞',)),
            tok('詳しく話しましょう。', '詳しい', ('形容詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '対し'])
        words = {'問題': object()}
        stage.add_cards(words)
        self.assertIn('問題', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に対し')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)

    def test_longer_form_keeps_its_identity(self):
        source = sentence('先生に対して丁寧に話しましょう。', [
            tok('先生', '先生', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('対し', '対する', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('丁寧に話しましょう。', '丁寧', ('名詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source]))
        words = {}
        stage.add_cards(words)
        self.assertIn('〜に対して', words)
        self.assertNotIn(RULE.expression, words)
