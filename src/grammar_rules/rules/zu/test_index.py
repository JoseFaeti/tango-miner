import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.zu.index import RULE, match_zu
from src.grammar_rules.rules.zuni.index import RULE as ZUNI_RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class ZuTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '何も言わず帰った。': True, '食べず帰った。': True,
            'せず帰った。': True, '来ず帰った。': True,
            '読まれず終わった。': True, '行かずに帰った。': True,
            '水を飲む。': False, '地図を見た。': False,
            'さえずる鳥。': False, 'ず帰った。': False, '行か ず帰った。': False,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_zu(aligned)],
                                 ['ず'] if expected else [])

    def test_reservation_example_and_overlap(self):
        parts = [tok('笑わ', '笑う', ('動詞', '一般', '*', '*', '五段-ワア行', '未然形-一般')),
                 tok('ず', 'ず', ('助動詞',)), tok('に', 'に', ('助詞',)),
                 tok('は', 'は', ('助詞',)), tok('い', 'いる', ('動詞',)),
                 tok('られ', 'られる', ('助動詞',)), tok('ない', 'ない', ('助動詞',))]
        stage = GrammarStep(rules=(RULE, ZUNI_RULE))
        result = ExcludeGrammarTokensStep(stage).process(
            stage.process(Artifact([sentence('笑わずにはいられない', parts)])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ず', 'に'])
        words = {'笑う': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ず')
        self.assertIn(ZUNI_RULE.expression, words)
        self.assertIn('笑う', words)
