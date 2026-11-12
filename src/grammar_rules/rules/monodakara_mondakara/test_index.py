import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import MONODAKARA_MONDAKARA_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.monodakara_mondakara.index import RULE, match_monodakara_mondakara
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MonodakaraMondakaraTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '電車が遅れたものだから遅刻した。': 'ものだから',
            '忙しいもんだから忘れた。': 'もんだから',
            '静かなものだから眠くなった。': 'ものだから',
            '子供なもんだから仕方ない。': 'もんだから',
            '買ったものだから大切にする。': 'ものだから',  # Also a literal thing.
            '私のものだから返して。': None,
            '買うものだ。': None,
            '行くから帰る。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_monodakara_mondakara(aligned)],
                                 [] if expected is None else [expected])

    def test_identity_card_and_boundaries(self):
        verb = tok('行く', '行く', ('動詞', '*', '*', '*', '*', '連体形-一般'))
        mono = tok('もの', 'もの', ('名詞',))
        copula = tok('だ', 'だ', ('助動詞',))
        kara = tok('から', 'から', ('助詞', '接続助詞'))
        source = sentence('行くものだから困る。', [verb, mono, copula, kara,
                          tok('困る。', '困る', ('動詞',))])
        stage = GrammarStep(rules=(RULE,))
        artifact = stage.process(Artifact([source, source]))
        reserved = ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertFalse(any(t.get('grammar_reserved') for t in reserved.data[0].tokens))
        words = {'行く': object(), 'もの': object()}
        stage.add_cards(words)
        self.assertIn('もの', words)
        self.assertEqual(words[MONODAKARA_MONDAKARA_EXPRESSION].frequency, 2)
        self.assertEqual(words[MONODAKARA_MONDAKARA_EXPRESSION].sentences[0].surface_form, 'ものだから')
        self.assertIn(RULE, RULES)
        for text, raw in [('行く ものだから', [verb, mono, copula, kara]),
                          ('行くものだ', [verb, mono, copula]),
                          ('行くものだから', [verb, mono, copula, tok('から', 'から', ('助詞', '格助詞'))])]:
            aligned, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_monodakara_mondakara(aligned)), [])
