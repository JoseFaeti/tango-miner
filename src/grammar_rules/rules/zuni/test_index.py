import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.zuni.index import RULE, match_zuni
from src.grammar_rules.rules.zunihairarenai.index import RULE as LONG_RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class ZuniTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '食べずに出かけた。': True, '行かずに帰った。': True,
            'せずに済んだ。': True, '来ずに帰った。': True,
            '事件に関わらずに暮らした。': True, '笑わずにはいられない。': True,
            '地図に印をつけた。': False, '水に入る。': False,
            'ずに帰った。': False, '行かず に帰った。': False,
            '行か ずに帰った。': False, '行かず帰った。': False,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_zuni(aligned)],
                                 ['ずに'] if expected else [])

    def test_reservation_example_and_overlap(self):
        parts = [tok('笑わ', '笑う', ('動詞', '一般', '*', '*', '*', '未然形-一般')),
                 tok('ず', 'ず', ('助動詞',)), tok('に', 'に', ('助詞',)),
                 tok('は', 'は', ('助詞',)), tok('い', 'いる', ('動詞',)),
                 tok('られ', 'られる', ('助動詞',)), tok('ない', 'ない', ('助動詞',))]
        stage = GrammarStep(rules=(RULE, LONG_RULE))
        result = ExcludeGrammarTokensStep(stage).process(
            stage.process(Artifact([sentence('笑わずにはいられない', parts)])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ず', 'に', 'は', 'い', 'られ', 'ない'])
        words = {'笑う': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ずに')
        self.assertIn(LONG_RULE.expression, words)
        self.assertIn('笑う', words)
