import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.toiumonodemonai.index import RULE, match_toiumonodemonai
from src.grammar_rules.rules.monodehanai.index import match_monodehanai
from src.grammar_rules.rules.toiu.index import match_toiu
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class ToiumonodemonaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '高ければいいというものでもない。': 'というものでもない',
            '高いと言うものでもない。': 'と言うものでもない',
            '努力すれば成功するというものでもありません。': 'というものでもありません',
            '安ければいいっていうものでもない。': 'っていうものでもない',
            'それが正しいというものでもなかった。': 'というものでもなかった',
            'それが正しいというものでもないです。': 'というものでもないです',
            '人の悪口を言うものではない。': None,
            '高ければいいというものだ。': None,
            '高ければいいというものではない。': None,
            '高ければいいという ものでもない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toiumonodemonai(aligned)],
                                 [] if expected is None else [expected])
                if expected:
                    self.assertEqual(list(match_monodehanai(aligned)), [])
                    self.assertEqual(list(match_toiu(aligned)), [])

    def test_identity_and_reservation(self):
        tokens = [tok('高い', '高い', ('形容詞',)), tok('と', 'と', ('助詞',)),
                  tok('いう', 'いう', ('動詞',)), tok('もの', 'もの', ('名詞',)),
                  tok('で', 'だ', ('助動詞',)), tok('も', 'も', ('助詞',)),
                  tok('ない', 'ない', ('形容詞',))]
        source = sentence('高いというものでもない', tokens)
        stage = GrammarStep(rules=(RULE,))
        artifact = stage.process(Artifact([source, source]))
        reserved = ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertFalse(reserved.data[0].tokens[0].get('grammar_reserved'))
        self.assertTrue(all(t.get('grammar_reserved') for t in reserved.data[0].tokens[1:]))
        words = {'もの': object()}
        stage.add_cards(words)
        self.assertIn(RULE, RULES)
        self.assertIn('もの', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'というものでもない')
