import unittest
from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.ya_toka.index import RULE, match_ya_toka


class YaTokaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'猫や犬を見た。': ['や'], '猫とか犬とか好き。': ['とか', 'とか'],
                 '映画とか見る。': ['とか'], 'やっと来た。': [],
                 '置いとかなきゃ。': [], 'なんとかする。': [], '猫 とか犬': []}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ya_toka(tokens)], expected)

    def test_reservation_and_identity(self):
        source = sentence('猫とか犬が好きです。', [tok('猫', '猫', ('名詞',)),
                          tok('と', 'と', ('助詞', '格助詞')),
                          tok('か', 'か', ('助詞', '副助詞')),
                          tok('犬', '犬', ('名詞',)), tok('が', 'が', ('助詞', '格助詞')), tok('好き', '好き', ('形状詞',)), tok('です', 'です', ('助動詞',)), tok('。', '。', ('補助記号', '句点'))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['と', 'か'])
        words = {'猫': object(), '犬': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'とか')
        self.assertIn('猫', words)
        self.assertIn('犬', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)