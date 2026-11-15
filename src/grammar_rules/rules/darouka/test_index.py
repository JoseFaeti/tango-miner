import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.darouka.index import RULE, match_darouka
from src.grammar_rules.rules.darou_deshou.index import RULE as DAROU_RULE
from src.grammar_rules.rules.nodarouka.index import RULE as NO_RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class DaroukaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '明日は雨だろうか。': 'だろうか',
            '彼は来るだろうか考える。': 'だろうか',
            'これは高いでしょうか。': 'でしょうか',
            '行くのだろうか。': 'だろうか',
            '私のだろうか。': 'だろうか',
            '明日は雨だろう。': None,
            '彼は来るかもしれない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_darouka(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_overlap_and_boundaries(self):
        verb = tok('行く', '行く', ('動詞', '一般', '*', '*', '五段-カ行', '終止形-一般'))
        marker = tok('の', 'の', ('助詞', '準体助詞'))
        copula = tok('だろう', 'だ', ('助動詞',))
        ka = tok('か', 'か', ('助詞', '終助詞'))
        source = sentence('どこへ行くのだろうか。', [tok('どこへ', 'どこ', ('名詞',)), verb, marker, copula, ka, tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(NO_RULE, RULE, DAROU_RULE))
        artifact = stage.process(Artifact([source, source]))
        reserved = ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in reserved.data[0].tokens
                          if t.get('grammar_reserved')], ['の', 'だろう', 'か'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'だろうか')
        self.assertIn(NO_RULE.expression, words)
        self.assertIn(DAROU_RULE.expression, words)
        for text, raw in [('だろう か', [copula, ka]),
                          ('だろうか', [tok('だろう', 'だ', ('名詞',)), ka]),
                          ('だろうか', [copula, tok('か', 'か', ('名詞',))])]:
            aligned, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_darouka(aligned)), [])
