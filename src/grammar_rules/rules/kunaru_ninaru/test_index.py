import unittest

from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.kunaru_ninaru.index import match_kunaru_ninaru
from src.grammar_rules.rules.te_iru.test_index import tok
from src.grammar_rules.rules.te_iru.test_index import sentence
from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import KUNARU_NINARU_EXPRESSION


class BecomeTests(unittest.TestCase):
    def test_card_and_lexical_tokens(self):
        source = sentence('今日は寒くなったと思います。', [
            tok('今日は', '今日', ('名詞',)),
            tok('寒く', '寒い', ('形容詞', '*', '*', '*', '*', '連用形-一般')),
            tok('なっ', 'なる', ('動詞',)), tok('た', 'た', ('助動詞',)),
            tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep()
        artifact = Artifact([source, source])
        stage.process(artifact)
        words = {}
        stage.add_cards(words)
        self.assertIn(KUNARU_NINARU_EXPRESSION, words)
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertFalse(artifact.data[0].tokens[1].get('grammar_reserved', False))

    def test_source_tokenizations(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '寒くなる。': '寒くなる', '寒くなった。': '寒くなった',
            '寒くなりません。': '寒くなりません', '寒くなってきた。': '寒くなって',
            '静かになる。': 'になる', '先生になった。': 'になった',
            '元気にならない。': 'にならない', '音が鳴る。': None,
            '本を読むようになる。': None, '行くことになる。': None,
            '雨が降らなくなる。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kunaru_ninaru(aligned)],
                                 [] if expected is None else [expected])

    def test_incomplete_and_separated(self):
        for text, raw in [
            ('寒くなっ', [tok('寒く', '寒い', ('形容詞', '*', '*', '*', '*', '連用形-一般')),
                        tok('なっ', 'なる', ('動詞',))]),
            ('寒く なる', [tok('寒く', '寒い', ('形容詞', '*', '*', '*', '*', '連用形-一般')),
                         tok('なる', 'なる', ('動詞',))]),
        ]:
            aligned, error = aligned_tokens(text, raw)
            if error is None:
                self.assertEqual(list(match_kunaru_ninaru(aligned)), [])


if __name__ == '__main__':
    unittest.main()
