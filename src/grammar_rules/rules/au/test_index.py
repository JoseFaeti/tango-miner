import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import AU_GRAMMAR_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.au.index import match_au
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class AuTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '話し合う。': '話し合う', '助け合った。': '助け合った',
            '見つめ合って。': '見つめ合って', '話し合います。': '話し合います',
            '話し合わなかった。': '話し合わなかった', '話し合えば。': '話し合えば',
            '友達に合う。': None, '友達に会う。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_au(aligned)],
                                 [] if expected is None else [expected])

    def test_split_and_incomplete(self):
        verb = tok('話し', '話す', ('動詞', '一般', '*', '*', '五段', '連用形-一般'))
        suffix = tok('合っ', '合う', ('動詞', '非自立可能'))
        past = tok('た', 'た', ('助動詞',))
        for raw, expected in [([verb, suffix, past], ['話し合った']),
                              ([verb, suffix], []),
                              ([tok('本', '本', ('名詞',)), suffix, past], [])]:
            text = ''.join(t['surface'] for t in raw)
            aligned, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual([text[a:b] for a, b in match_au(aligned)], expected)

    def test_card_preserves_lexical_candidate(self):
        source = sentence('私たちは皆で話し合うと思います。', [tok('私たちは皆で', '皆', ('名詞',)),
                          tok('話し合う', '話し合う', ('動詞',)), tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'話し合う': object()}
        stage.add_cards(words)
        self.assertIn('話し合う', words)
        self.assertEqual(words[AU_GRAMMAR_EXPRESSION].frequency, 2)
        self.assertEqual(words[AU_GRAMMAR_EXPRESSION].sentences[0].surface_form, '話し合う')
        artifact = ExcludeGrammarTokensStep(stage).process(Artifact([source, source]))
        self.assertFalse(artifact.data[0].tokens[1].get('grammar_reserved', False))
