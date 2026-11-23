import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.karayuuto_karaieba.index import (
    RULE,
    match_karayuuto_karaieba,
)
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class KarayuutoKaraiebaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '値段から言うと安い。': 'から言うと',
            '私から言えば簡単だ。': 'から言えば',
            '結果からいうと成功だ。': 'からいうと',
            '彼からいえば当然だ。': 'からいえば',
            '食べてから言うと決めた。': None,
            '私から 言うと伝えた。': None,
            '私から言う と伝えた。': None,
            '私から言った。': None,
            '私から言えばかりが聞こえる。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_karayuuto_karaieba(aligned)],
                                 [expected] if expected else [], raw)

    def test_card_identity_and_reservation_preserve_noun(self):
        source = sentence('私から言えば簡単だ。', [
            tok('私', '私', ('代名詞',)),
            tok('から', 'から', ('助詞', '格助詞')),
            tok('言え', '言う', ('動詞', '一般')),
            tok('ば', 'ば', ('助詞', '接続助詞')),
            tok('簡単だ。', '簡単', ('名詞',))])
        stage = GrammarStep(rules=(RULE,))
        artifact = stage.process(Artifact([source]))
        words = {'私': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'から言えば')
        self.assertIn('私', words)
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens
                          if t.get('grammar_reserved')], ['から', '言え', 'ば'])
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
