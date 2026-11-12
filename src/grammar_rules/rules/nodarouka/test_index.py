import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import NODAROUKA_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.nodarouka.index import RULE, match_nodarouka
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NodaroukaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            'なぜ行くのだろうか。': 'のだろうか',
            'どこへ行ったんだろうか。': 'んだろうか',
            '高いのでしょうか。': 'のでしょうか',
            '静かなのだろうか。': 'のだろうか',
            '学生なんでしょうか。': 'んでしょうか',
            '何を買うのだろうか考える。': 'のだろうか',
            '私のだろうか。': None, '行くのだろう。': None,
            '行くだろうか。': None, '私のかばん。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nodarouka(aligned)],
                                 [] if expected is None else [expected])

    def test_card_reservation_and_boundary(self):
        verb = tok('行く', '行く', ('動詞', '*', '*', '*', '*', '連体形-一般'))
        marker = tok('の', 'の', ('助詞', '準体助詞'))
        copula = tok('だろう', 'だ', ('助動詞',))
        ka = tok('か', 'か', ('助詞', '終助詞'))
        source = sentence('どこへ行くのだろうか。', [tok('どこへ', 'どこ', ('名詞',)),
                          verb, marker, copula, ka, tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        artifact = stage.process(Artifact([source, source]))
        reserved = ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in reserved.data[0].tokens
                          if t.get('grammar_reserved')], ['の', 'だろう', 'か'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[NODAROUKA_EXPRESSION].frequency, 2)
        self.assertEqual(words[NODAROUKA_EXPRESSION].sentences[0].surface_form, 'のだろうか')
        aligned, error = aligned_tokens('行く のだろうか', [verb, marker, copula, ka])
        self.assertIsNone(error)
        self.assertEqual(list(match_nodarouka(aligned)), [])
