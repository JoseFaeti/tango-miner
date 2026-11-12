import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import OWARU_OERU_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.owaru_oeru.index import match_owaru_oeru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class OwaruOeruTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '本を読み終わる。': '読み終わる', '本を読み終わった。': '読み終わった',
            '食べ終わって。': '食べ終わって', '読み終わります。': '読み終わります',
            '読み終わらない。': '読み終わらない', '読み終われば。': '読み終われば',
            '本を読み終える。': '読み終える', '書き終えた。': '書き終えた',
            '書き終えて。': '書き終えて', '書き終えます。': '書き終えます',
            '書き終えなかった。': '書き終えなかった', '書き終えれば。': '書き終えれば',
            '仕事が終わる。': None, '仕事を終える。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_owaru_oeru(aligned)],
                                 [] if expected is None else [expected])

    def test_split_tokens_and_incomplete_forms(self):
        verb = tok('読み', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-一般'))
        suffix = tok('終わっ', '終わる', ('動詞', '非自立可能'))
        past = tok('た', 'た', ('助動詞',))
        for raw, expected in [([verb, suffix, past], ['読み終わった']),
                              ([verb, suffix], []),
                              ([tok('本', '本', ('名詞',)), suffix, past], [])]:
            text = ''.join(t['surface'] for t in raw)
            aligned, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual([text[a:b] for a, b in match_owaru_oeru(aligned)], expected)
        aligned, error = aligned_tokens('読み 終わった', [verb, suffix, past])
        self.assertIsNone(error)
        self.assertEqual(list(match_owaru_oeru(aligned)), [])

    def test_card_preserves_lexical_candidate(self):
        source = sentence('私は本を読み終わると思います。', [tok('私は本を', '本', ('名詞',)),
                          tok('読み終わる', '読み終わる', ('動詞',)), tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'読み終わる': object()}
        stage.add_cards(words)
        self.assertIn('読み終わる', words)
        self.assertEqual(words[OWARU_OERU_EXPRESSION].frequency, 2)
        self.assertEqual(words[OWARU_OERU_EXPRESSION].sentences[0].surface_form, '読み終わる')
        artifact = ExcludeGrammarTokensStep(stage).process(Artifact([source]))
        self.assertFalse(artifact.data[0].tokens[1].get('grammar_reserved', False))
