import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, NAIKANA_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.naikana.index import RULE, match_naikana
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NaikanaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨が降らないかな。': 'ないかな',
            '手伝ってくれないかなあ。': 'ないかなあ',
            '何かいい方法はないかな。': 'ないかな',
            'これ高くないかな。': 'ないかな',
            '行くかな。': None, '行かないか。': None,
            '行かないかしら。': None, '魚かな。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_naikana(aligned)],
                                 [] if expected is None else [expected])

    def test_boundaries_identity_and_reservation(self):
        verb = tok('行か', '行く', ('動詞',))
        negative = tok('ない', 'ない', ('助動詞',))
        ka = tok('か', 'か', ('助詞', '終助詞'))
        na = tok('な', 'な', ('助詞', '終助詞'))
        for text, parts in [('行かない かな', [verb, negative, ka, na]),
                            ('行かないか な', [verb, negative, ka, na]),
                            ('行かないかな', [verb, tok('ない', 'ない', ('名詞',)), ka, na])]:
            aligned, error = aligned_tokens(text, parts)
            self.assertIsNone(error)
            self.assertEqual(list(match_naikana(aligned)), [])
        source = sentence('明日は皆で行かないかな。', [
            tok('明日は皆で', '皆', ('名詞',)), verb, negative, ka, na,
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        artifact = stage.process(Artifact([source, source]))
        reserved = ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in reserved.data[0].tokens
                          if t.get('grammar_reserved')], ['ない', 'か', 'な'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertIn(RULE, RULES)
        self.assertIn(NAIKANA_EXPRESSION, GRAMMAR_EXPRESSIONS)
        self.assertEqual(words[NAIKANA_EXPRESSION].frequency, 2)
        self.assertEqual(words[NAIKANA_EXPRESSION].sentences[0].surface_form, 'ないかな')
