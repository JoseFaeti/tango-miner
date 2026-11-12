import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.GrammarConstants import MONO_MON_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.mono_mon.index import match_mono_mon
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MonoMonTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            'だって行きたくないもの。': 'もの', 'だって疲れたもん。': 'もん',
            '高いもの。': 'もの', '静かなもん。': 'もん',
            '買ったものを見せて。': None, '私のもの。': None,
            '行くものか。': None, '行くものだから困る。': None,
            '行くものだ。': None, '行くもんね。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_mono_mon(aligned)],
                                 [] if expected is None else [expected])

    def test_card_retains_components_and_requires_adjacency(self):
        verb = tok('行く', '行く', ('動詞', '一般', '*', '*', '五段', '終止形-一般'))
        mono = tok('もん', 'もん', ('名詞',))
        source = sentence('私はあの場所へ行くもん。', [tok('私はあの場所へ', '私', ('名詞',)), verb, mono, tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'行く': object(), 'もん': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertIn('もん', words)
        self.assertEqual(words[MONO_MON_EXPRESSION].frequency, 2)
        self.assertEqual(words[MONO_MON_EXPRESSION].sentences[0].surface_form, 'もん')
        aligned, error = aligned_tokens('行く もん', [verb, mono])
        self.assertIsNone(error)
        self.assertEqual(list(match_mono_mon(aligned)), [])
