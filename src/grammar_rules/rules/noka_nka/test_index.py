import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import NOKA_NKA_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.noka_nka.index import RULE, match_noka_nka
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NokaNkaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行くのか。': 'のか', '行くんか。': 'んか',
            '高かったのか。': 'のか', '静かなのか。': 'のか',
            '学生なのか。': 'のか', '何を買ったのか分からない。': 'のか',
            '行くのかどうか。': 'のか', '私のかばん。': None,
            'なんか食べたい。': None, 'そんなものか。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_noka_nka(aligned)],
                                 [] if expected is None else [expected])

    def test_card_and_boundaries(self):
        verb = tok('行く', '行く', ('動詞', '*', '*', '*', '*', '連体形-一般'))
        marker = tok('の', 'の', ('助詞', '準体助詞'))
        ka = tok('か', 'か', ('助詞', '終助詞'))
        source = sentence('あの場所へ行くのか。', [
            tok('あの場所へ', '場所', ('名詞',)), verb, marker, ka,
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        artifact = stage.process(Artifact([source, source]))
        reserved = ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in reserved.data[0].tokens
                          if t.get('grammar_reserved')], ['の', 'か'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[NOKA_NKA_EXPRESSION].frequency, 2)
        self.assertEqual(words[NOKA_NKA_EXPRESSION].sentences[0].surface_form, 'のか')
        aligned, error = aligned_tokens('行く のか', [verb, marker, ka])
        self.assertIsNone(error)
        self.assertEqual(list(match_noka_nka(aligned)), [])
