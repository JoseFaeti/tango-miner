import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.nikatakunai.index import RULE, match_nikatakunai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NikatakunaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '想像に難くない。': ['に難くない'], '想像にかたくない。': ['にかたくない'],
            '推察するに難くない。': ['に難くない'], '想像に難くなかった。': ['に難くなかった'],
            '想像に難くないです。': ['に難くないです'],
            '想像に難くなく、驚かなかった。': ['に難くなく'],
            '想像に難くなければ納得できる。': ['に難くなければ'],
            '石が硬くない。': [], '想像に硬くない。': [], '想像に難い。': [],
            '想像に 難くない。': [], '困難に難くない。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nikatakunai(tokens)], expected)

    def test_reservation_and_identity(self):
        source = sentence('その結果は想像に難くないと思います。', [tok('その結果は', '結果', ('名詞',)),
                          tok('想像', '想像', ('名詞',)),
                          tok('に', 'に', ('助詞',)), tok('難く', '難い', ('形容詞',)),
                          tok('ない', 'ない', ('形容詞',)), tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '難く', 'ない'])
        words = {'想像': object()}
        stage.add_cards(words)
        self.assertIn('想像', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に難くない')
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)

    def test_default_registry_collects_expression_and_particle(self):
        source = sentence('その結果は想像に難くないと思います。', [
            tok('その結果は', '結果', ('名詞',)), tok('想像', '想像', ('名詞',)),
            tok('に', 'に', ('助詞', '格助詞')), tok('難く', '難い', ('形容詞',)),
            tok('ない', 'ない', ('形容詞',)), tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source]))
        words = {'想像': object()}
        stage.add_cards(words)
        self.assertIn(RULE.expression, words)
        self.assertIn('〜に', words)
        self.assertIn('想像', words)
