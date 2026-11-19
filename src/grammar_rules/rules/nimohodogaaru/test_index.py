import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.nimohodogaaru.index import RULE, match_nimohodogaaru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NimoHodoGaAruTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '冗談にもほどがある。': 'にもほどがある',
            '失礼にも程がある。': 'にも程がある',
            '馬鹿にもほどがあった。': 'にもほどがあった',
            '失礼にもほどがあります。': 'にもほどがあります',
            '失礼にもほどがありました。': 'にもほどがありました',
            '失礼にもほどがありません。': None,
            '冗談のほどがある。': None,
            'ほどがある。': None,
            '冗談にもほどがない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nimohodogaaru(aligned)],
                                 [] if expected is None else [expected], raw)

    def test_card_retains_attachment_and_reserves_phrase(self):
        raw = [tok('冗談', '冗談', ('名詞',)), tok('に', 'に', ('助詞',)),
               tok('も', 'も', ('助詞',)), tok('ほど', 'ほど', ('名詞',)),
               tok('が', 'が', ('助詞',)), tok('ある', 'ある', ('動詞',))]
        raw.append(tok('と思います。', '思う', ('動詞',)))
        source = sentence('冗談にもほどがあると思います。', raw)
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertFalse(result.data[0].tokens[0].get('grammar_reserved', False))
        self.assertTrue(all(t.get('grammar_reserved') for t in result.data[0].tokens[1:-1]))
        self.assertFalse(result.data[0].tokens[-1].get('grammar_reserved', False))
        words = {'冗談': object()}
        stage.add_cards(words)
        self.assertIn('冗談', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'にもほどがある')


if __name__ == '__main__':
    unittest.main()
