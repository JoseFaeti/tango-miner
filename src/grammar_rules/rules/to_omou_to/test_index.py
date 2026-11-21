import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.to_omou_to.index import RULE, match_to_omou_to
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class ToOmouToTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '明日帰ると思うと寂しい。': 'と思うと',
            'もう会えないと思うと悲しい。': 'と思うと',
            '行くとおもうと嬉しい。': 'とおもうと',
            '帰ったかと思うと出かけた。': None,
            '明日帰ると思った。': None,
            'そう思うと嬉しい。': None,
            '帰ると 思うと寂しい。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_to_omou_to(tokens)],
                                 [] if expected is None else [expected])

    def test_registry_reservation_and_overlap(self):
        source = sentence('帰ると思うと寂しい。', [
            tok('帰る', '帰る', ('動詞', '一般', '*', '*', '五段', '終止形-一般')),
            tok('と', 'と', ('助詞', '格助詞')),
            tok('思う', '思う', ('動詞', '一般')),
            tok('と', 'と', ('助詞', '接続助詞')),
            tok('寂しい。', '寂しい', ('形容詞',))])
        stage = GrammarStep()
        result = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(result)
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        self.assertEqual(stage.occurrences['to_omou'].count, 0)
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['と', '思う', 'と'])
        self.assertEqual(stage.occurrences[RULE.id].examples[0].surface_form, 'と思うと')
