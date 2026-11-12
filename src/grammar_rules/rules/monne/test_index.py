import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import MONNE_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.monne.index import RULE, match_monne
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MonneTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '疲れたもんね。': 'もんね', '行かないもんね。': 'もんね',
            '高いもんね。': 'もんね', '静かなもんね。': 'もんね',
            '買ったもんね。': 'もんね',  # Can also refer to a purchased thing.
            '行くもん。': None, '行くもんか。': None,
            'そんなもんね。': None, '行くものね。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_monne(aligned)],
                                 [] if expected is None else [expected])

    def test_card_components_and_boundaries(self):
        verb = tok('行く', '行く', ('動詞', '*', '*', '*', '*', '連体形-一般'))
        mon = tok('もん', 'もん', ('名詞',))
        ne = tok('ね', 'ね', ('助詞', '終助詞'))
        source = sentence('私はあの場所へ行くもんね。', [
            tok('私はあの場所へ', '場所', ('名詞',)), verb, mon, ne,
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        artifact = stage.process(Artifact([source, source]))
        reserved = ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertFalse(any(t.get('grammar_reserved') for t in reserved.data[0].tokens))
        words = {'行く': object(), 'もん': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertIn('もん', words)
        self.assertEqual(words[MONNE_EXPRESSION].frequency, 2)
        self.assertEqual(words[MONNE_EXPRESSION].sentences[0].surface_form, 'もんね')
        for text in ('行く もんね', '行くもん ね'):
            aligned, error = aligned_tokens(text, [verb, mon, ne])
            self.assertIsNone(error)
            self.assertEqual(list(match_monne(aligned)), [])
