import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.toatsute.index import match_toatsute
from src.grammar_rules.rules.toatsuteha.index import RULE, match_toatsuteha
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class ToatsutehaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '子供のためとあっては断れない。': 'とあっては',
            '休日とあっては仕方がない。': 'とあっては',
            '友達と会っては話す。': None,
            '休日とあって混んでいる。': None,
            '休日とあっても困る。': None,
            '休日とあって は困る。': None,
            '休日と あっては困る。': None,
            'とあっては困る。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toatsuteha(tokens)],
                                 [] if expected is None else [expected])
                if expected:
                    self.assertEqual(list(match_toatsute(tokens)), [])

    def test_reservation_and_identity(self):
        source = sentence('休日とあっては困る。', [
            tok('休日', '休日', ('名詞',)), tok('と', 'と', ('助詞',)),
            tok('あっ', 'ある', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('は', 'は', ('助詞',)), tok('困る', '困る', ('動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['と', 'あっ', 'て', 'は'])
        words = {'休日': object()}
        stage.add_cards(words)
        self.assertIn('休日', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'とあっては')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
