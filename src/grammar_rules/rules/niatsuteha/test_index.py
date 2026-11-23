import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.niatsute.index import RULE as NIATSUTE
from src.grammar_rules.rules.niatsuteha.index import RULE, match_niatsuteha
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NiatsutehaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '会議にあっては発言する。': 'にあっては',
            '危機にあっては冷静さが必要だ。': 'にあっては',
            '事故にあっては困る。': None,
            '状況に有っては難しい。': 'に有っては',
            '友達に会っては話す。': None,
            '危機にあっても希望を持つ。': None,
            '危機にあって希望を持つ。': None,
            '危機にあって は困る。': None,
            '危機に あっては困る。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_niatsuteha(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_overlap(self):
        source = sentence('危機にあっては冷静だ。', [
            tok('危機', '危機', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('あっ', 'ある', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('は', 'は', ('助詞',)), tok('冷静だ。', '冷静', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'あっ', 'て', 'は'])
        words = {'危機': object()}
        stage.add_cards(words)
        self.assertIn('危機', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'にあっては')
        self.assertEqual(words[NIATSUTE.expression].sentences[0].surface_form, 'にあって')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
