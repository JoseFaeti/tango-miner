import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.nu_n.index import RULE, match_nu_n
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NuNTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'行かぬ。': 'ぬ', '行かん。': 'ん', '食べぬ人。': 'ぬ',
                 '食べん。': 'ん', 'せぬ。': 'ぬ', '来ぬ。': 'ぬ',
                 '来ん。': 'ん', '読まれぬ。': 'ぬ', '行かぬまま。': 'ぬ',
                 '死ぬ。': None, '読んだ。': None, '行くんだ。': None,
                 'ありません。': None, '行か ぬ。': None, '行かんかった。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nu_n(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_card(self):
        source = sentence('私は本を読まぬと決めた。', [tok('私は本を', '本', ('名詞',)),
            tok('読ま', '読む', ('動詞', '*', '*', '*', '五段-マ行', '未然形-一般')),
            tok('ぬ', 'ぬ', ('助動詞',)), tok('と決めた。', '決める', ('動詞',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')], ['ぬ'])
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ぬ')
        self.assertIn('読む', words)
