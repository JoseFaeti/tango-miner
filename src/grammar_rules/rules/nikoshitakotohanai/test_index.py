import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.nikoshitakotohanai.index import RULE, match_nikoshitakotohanai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NikoshitakotohanaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '早いに越したことはない。': 'に越したことはない',
            '行くに越したことはない。': 'に越したことはない',
            '行かないに越したことはありません。': 'に越したことはありません',
            '安全に越したことはなかった。': 'に越したことはなかった',
            '早いに越したことはある。': None,
            '早いに越すことはない。': None,
            '早いに越したことは ない。': None,
            '山を越したことはない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nikoshitakotohanai(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('早いに越したことはない。', [
            tok('早い', '早い', ('形容詞',)), tok('に', 'に', ('助詞',)),
            tok('越し', '越す', ('動詞',)), tok('た', 'た', ('助動詞',)),
            tok('こと', 'こと', ('名詞',)), tok('は', 'は', ('助詞',)),
            tok('ない', 'ない', ('形容詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '越し', 'た', 'こと', 'は', 'ない'])
        self.assertFalse(result.data[0].tokens[0].get('grammar_reserved'))
        words = {'早い': object()}
        stage.add_cards(words)
        self.assertIn('早い', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に越したことはない')
        from src.GrammarConstants import GRAMMAR_EXPRESSIONS
        from src.grammar_rules.registry import RULES
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertIn(RULE, RULES)
