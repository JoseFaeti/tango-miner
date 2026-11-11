import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import HOSHII_GRAMMAR_EXPRESSION as EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.hoshii.index import match_hoshii
from src.grammar_rules.rules.te_hoshii.test_index import TeHoshiiTests
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class HoshiiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        cases = {
            '新しい本が欲しいです。': '欲しいです',
            '本がほしかった。': 'ほしかった',
            '本が欲しくない。': '欲しくない',
            '本が欲しければ言って。': '欲しければ',
            '水が欲しくて来た。': '欲しくて',
            '本を読んでほしい。': None,
            '星がきれいだ。': None,
            '本を干している。': None,
        }
        helper = TeHoshiiTests()
        for text, expected in cases.items():
            with self.subTest(text=text):
                tokens, error = aligned_tokens(text, helper.sudachi_tokens(text))
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_hoshii(tokens)],
                                 [] if expected is None else [expected])

    def test_incomplete_and_wrong_pos(self):
        for token in (tok('ほしかっ', '欲しい', ('形容詞',)),
                      tok('ほしい', '欲しい', ('名詞',))):
            tokens, error = aligned_tokens(token['surface'], [token])
            self.assertIsNone(error)
            self.assertEqual(list(match_hoshii(tokens)), [])

    def test_card_and_reservation(self):
        text = '私は新しい本が欲しいと思います。'
        source = sentence(text, TeHoshiiTests().sudachi_tokens(text))
        stage = GrammarStep()
        artifact = Artifact([source])
        stage.process(artifact)
        ExcludeGrammarTokensStep(stage).process(artifact)
        tokens = artifact.data[0].tokens
        self.assertFalse(next(t for t in tokens if t['surface'] == '本').get('grammar_reserved', False))
        self.assertFalse(next(t for t in tokens if t['surface'] == '欲しい').get('grammar_reserved', False))
        words = {'本': object()}
        stage.add_cards(words)
        self.assertIn('本', words)
        self.assertEqual(words[EXPRESSION].sentences[0].surface_form, '欲しい')
