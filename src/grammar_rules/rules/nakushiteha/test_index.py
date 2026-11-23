import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.nakushite.index import RULE as SHORT_RULE
from src.grammar_rules.rules.nakushiteha.index import RULE, match_nakushiteha
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NakushitehaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼の協力なくしては成功しない。': True,
            '努力なくしては実現できない。': True,
            '努力なくして成功はない。': False,
            '財布をなくしては困る。': False,
            '努力なくして は成功しない。': False,
            '努力 なくしては成功しない。': False,
            'なくしては成功しない。': False,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nakushiteha(aligned)],
                                 ['なくしては'] if expected else [])

    def test_overlap_reservation_and_identity(self):
        parts = [tok('努力', '努力', ('名詞',)), tok('なく', 'ない', ('形容詞',)),
                 tok('し', 'する', ('動詞',)), tok('て', 'て', ('助詞',)),
                 tok('は', 'は', ('助詞',)), tok('成功', '成功', ('名詞',)),
                 tok('しない', 'する', ('動詞',))]
        stage = GrammarStep(rules=(SHORT_RULE, RULE))
        result = ExcludeGrammarTokensStep(stage).process(
            stage.process(Artifact([sentence('努力なくしては成功しない', parts)])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['なく', 'し', 'て', 'は'])
        words = {'努力': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'なくしては')
        self.assertEqual(words[SHORT_RULE.expression].sentences[0].surface_form, 'なくして')
        self.assertIn('努力', words)
