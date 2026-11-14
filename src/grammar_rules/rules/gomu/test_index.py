import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GOMU_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.gomu.index import match_gomu
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class GomuTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '飛び込む。': '飛び込む', '書き込んだ。': '書き込んだ',
            '飲み込んで。': '飲み込んで', '考え込みます。': '考え込みます',
            '考え込まなかった。': '考え込まなかった', '飛び込めば。': '飛び込めば',
            '道が込む。': None, '混む。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_gomu(aligned)],
                                 [] if expected is None else [expected])

    def test_split_and_incomplete(self):
        verb = tok('飛び', '飛ぶ', ('動詞', '一般', '*', '*', '五段', '連用形-一般'))
        suffix = tok('込ん', '込む', ('動詞', '非自立可能'))
        past = tok('だ', 'だ', ('助動詞',))
        for raw, expected in [([verb, suffix, past], ['飛び込んだ']),
                              ([verb, suffix], []),
                              ([tok('本', '本', ('名詞',)), suffix, past], [])]:
            text = ''.join(t['surface'] for t in raw)
            aligned, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual([text[a:b] for a, b in match_gomu(aligned)], expected)

    def test_card_preserves_lexical_candidate(self):
        source = sentence('私たちは皆で書き込むと思います。', [tok('私たちは皆で', '皆', ('名詞',)),
                          tok('書き込む', '書き込む', ('動詞',)), tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'書き込む': object()}
        stage.add_cards(words)
        self.assertIn('書き込む', words)
        self.assertEqual(words[GOMU_EXPRESSION].frequency, 2)
        self.assertEqual(words[GOMU_EXPRESSION].sentences[0].surface_form, '書き込む')
        artifact = ExcludeGrammarTokensStep(stage).process(Artifact([source, source]))
        self.assertFalse(artifact.data[0].tokens[1].get('grammar_reserved', False))
