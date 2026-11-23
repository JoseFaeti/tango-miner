import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.nitarinai.index import RULE, match_nitarinai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class NitarinaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '注目に足りない。': ['に足りない'], '語るに足りない。': ['に足りない'],
            '注目に足りなかった。': ['に足りなかった'],
            '注目に足りなくて残念だ。': ['に足りなく'],
            '注目に足りなければやめる。': ['に足りなければ'],
            '注目に足りません。': ['に足りません'],
            '注目に足りませんでした。': ['に足りませんでした'],
            '生活に足りない。': ['に足りない'],
            'お金が足りない。': [], '信頼に足る人物。': [],
            '注目に 足りない。': [], '注目に足りる。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nitarinai(tokens)], expected)

    def test_reservation_and_identity(self):
        source = sentence('それは注目に足りない話だと思う。', [tok('それは', 'それ', ('名詞',)),
                          tok('注目', '注目', ('名詞',)),
                          tok('に', 'に', ('助詞',)), tok('足り', '足りる', ('動詞',)),
                          tok('ない', 'ない', ('助動詞',)), tok('話だと思う。', '話', ('名詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '足り', 'ない'])
        words = {'注目': object()}
        stage.add_cards(words)
        self.assertIn('注目', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に足りない')
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
