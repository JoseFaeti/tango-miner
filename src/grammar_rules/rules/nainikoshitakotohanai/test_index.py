import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nainikoshitakotohanai.index import RULE
from src.grammar_rules.rules.nikoshitakotohanai.index import RULE as GENERAL
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class NainikoshitakotohanaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行かないに越したことはない。': 'ないに越したことはない',
            '怪我がないに越したことはありません。': 'ないに越したことはありません',
            '高くないに越したことはなかった。': 'ないに越したことはなかった',
            '早いに越したことはない。': None,
            '行かないに越すことはない。': None,
            '行かない に越したことはない。': None,
            '行かないに越したことはある。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in RULE.match(aligned)],
                                 [] if expected is None else [expected])

    def test_identity_reservation_and_general_overlap(self):
        source = sentence('行かないに越したことはない。', [
            tok('行か', '行く', ('動詞',)), tok('ない', 'ない', ('助動詞',)),
            tok('に', 'に', ('助詞',)), tok('越し', '越す', ('動詞',)),
            tok('た', 'た', ('助動詞',)), tok('こと', 'こと', ('名詞',)),
            tok('は', 'は', ('助詞',)), tok('ない', 'ない', ('形容詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, GENERAL))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ない', 'に', '越し', 'た', 'こと', 'は', 'ない'])
        self.assertFalse(result.data[0].tokens[0].get('grammar_reserved'))
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ないに越したことはない')
        self.assertIn(GENERAL.expression, words)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertIn(RULE, RULES)
