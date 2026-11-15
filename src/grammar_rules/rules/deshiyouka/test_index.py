import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.darouka.index import RULE as DAROUKA_RULE
from src.grammar_rules.rules.deshiyouka.index import RULE, match_deshiyouka
from src.grammar_rules.rules.nodarouka.index import RULE as NO_RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class DeshiyoukaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            'これは何でしょうか。': 'でしょうか',
            '明日は雨でしょうか。': 'でしょうか',
            '行くのでしょうか。': 'でしょうか',
            '彼は来るでしょうか考える。': 'でしょうか',
            '明日は雨だろうか。': None,
            '明日は雨でしょう。': None,
            '本ですか。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_deshiyouka(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_overlap_and_boundaries(self):
        verb = tok('行く', '行く', ('動詞', '*', '*', '*', '*', '連体形-一般'))
        marker = tok('の', 'の', ('助詞', '準体助詞'))
        copula = tok('でしょう', 'です', ('助動詞',))
        ka = tok('か', 'か', ('助詞', '終助詞'))
        source = sentence('どこへ行くのでしょうか。', [tok('どこへ', 'どこ', ('名詞',)),
                          verb, marker, copula, ka, tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(NO_RULE, DAROUKA_RULE, RULE))
        artifact = stage.process(Artifact([source, source]))
        reserved = ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in reserved.data[0].tokens
                          if t.get('grammar_reserved')], ['の', 'でしょう', 'か'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'でしょうか')
        self.assertIn(DAROUKA_RULE.expression, words)
        self.assertIn(NO_RULE.expression, words)
        for text, raw in [('でしょう か', [copula, ka]),
                          ('でしょうか', [tok('でしょう', 'です', ('名詞',)), ka]),
                          ('でしょうか', [copula, tok('か', 'か', ('名詞',))])]:
            tokens, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_deshiyouka(tokens)), [])
