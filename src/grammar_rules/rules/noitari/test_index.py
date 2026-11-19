import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.noitari.index import RULE, match_noitari
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NoitariTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'感激の至りです。': 'の至り',
                 '若気の至りだ。': None,
                 '感激のいたりです。': None,
                 '光栄の至りに存じます。': 'の至り',
                 '結論に至りました。': None,
                 '至りを知る。': None,
                 '感激の 至りだ。': None,
                 '感激の極みだ。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_noitari(tokens)],
                                 [] if expected is None else [expected])

    def test_kana_noun_and_verb_control(self):
        text = '感激のいたり'
        for lemma, pos, expected in [('いたり', '名詞', ['のいたり']),
                                     ('いたる', '動詞', [])]:
            raw = [tok('感激', '感激', ('名詞',)), tok('の', 'の', ('助詞',)),
                   tok('いたり', lemma, (pos,))]
            tokens, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual([text[a:b] for a, b in match_noitari(tokens)], expected)

    def test_reservation_and_identity(self):
        source = sentence('このようなお言葉をいただき感激の至りです。', [
            tok('このようなお言葉をいただき', 'いただく', ('動詞',)),
            tok('感激', '感激', ('名詞',)), tok('の', 'の', ('助詞',)),
            tok('至り', '至り', ('名詞',)), tok('です', 'です', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['の', '至り', 'です'])
        words = {'感激': object()}
        stage.add_cards(words)
        self.assertIn('感激', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'の至り')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
