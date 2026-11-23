import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.karamiruto_karamireba.index import (
    RULE,
    match_karamiruto_karamireba,
)
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class KaramirutoKaramirebaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '親から見ると心配だ。': 'から見ると',
            '私から見れば簡単だ。': 'から見れば',
            '値段からみると安い。': 'からみると',
            '彼からみれば当然だ。': 'からみれば',
            '食べてから見ると決めた。': None,
            '親から 見ると心配だ。': None,
            '親から見る と心配だ。': None,
            '親から見た話だ。': None,
            '親から見られる。': None,
            '親から見ればかり気にする。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_karamiruto_karamireba(aligned)],
                                 [expected] if expected else [], raw)

    def test_card_identity_and_reservation_preserve_noun(self):
        source = sentence('親から見れば心配だ。', [
            tok('親', '親', ('名詞', '普通名詞')),
            tok('から', 'から', ('助詞', '格助詞')),
            tok('見れ', '見る', ('動詞', '非自立可能')),
            tok('ば', 'ば', ('助詞', '接続助詞')),
            tok('心配だ。', '心配', ('名詞',))])
        stage = GrammarStep(rules=(RULE,))
        artifact = stage.process(Artifact([source]))
        words = {'親': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'から見れば')
        self.assertIn('親', words)
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens
                          if t.get('grammar_reserved')], ['から', '見れ', 'ば'])
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
