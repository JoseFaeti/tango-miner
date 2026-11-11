import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import ARU_IRU_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.aru_iru.index import RULE, match_aru_iru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class AruIruTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '庭に猫がいる。': 'いる', '庭に猫がいた。': 'いた',
            '庭に猫がいない。': 'いない', '庭に猫がいませんでした。': 'いませんでした',
            '庭に木がある。': 'ある', '庭に木があった。': 'あった',
            '庭に木があります。': 'あります', '庭に木があればいい。': 'あれば',
            '本を読んでいる。': None, '机に置いてある。': None,
            'お金が要る。': None, '家に入る。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_aru_iru(tokens)],
                                 [] if expected is None else [expected])

    def test_lexical_tokens_remain_available(self):
        source = sentence('広い庭に猫がいると思います。', [tok('広い', '広い', ('形容詞',)), tok('庭', '庭', ('名詞',)),
            tok('に', 'に', ('助詞', '格助詞')), tok('猫', '猫', ('名詞',)),
            tok('が', 'が', ('助詞', '格助詞')),
            tok('いる', 'いる', ('動詞', '非自立可能')), tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep(rules=(RULE,))
        artifact = Artifact([source])
        stage.process(artifact)
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertFalse(any(t.get('grammar_reserved') for t in artifact.data[0].tokens))
        words = {'いる': object()}
        stage.add_cards(words)
        self.assertIn('いる', words)
        self.assertEqual(words[ARU_IRU_EXPRESSION].sentences[0].surface_form, 'いる')
