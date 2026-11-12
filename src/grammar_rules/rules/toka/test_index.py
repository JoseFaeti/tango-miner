import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.toka.index import RULE, match_toka
from src.grammar_rules.rules.ya_toka.index import match_ya_toka


class TokaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼は明日来るとか。': ['とか'], '彼は帰ったとか聞いた。': ['とか'],
            '高いとか言っていた。': ['とか'], '休みだとか聞いた。': ['とか'],
            '猫とか犬が好き。': [], 'なんとかする。': [], '置いとかなきゃ。': [],
            '来る とか聞いた。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toka(tokens)], expected)
                if text == '猫とか犬が好き。':
                    self.assertEqual([text[a:b] for a, b in match_ya_toka(tokens)], ['とか'])

    def test_reservation_and_identity(self):
        source = sentence('彼は来るとか聞いた。', [
            tok('彼', '彼', ('代名詞',)), tok('は', 'は', ('助詞',)),
            tok('来る', '来る', ('動詞',)), tok('と', 'と', ('助詞', '格助詞')),
            tok('か', 'か', ('助詞', '副助詞')), tok('聞い', '聞く', ('動詞',)),
            tok('た', 'た', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')], ['と', 'か'])
        words = {'来る': object()}
        stage.add_cards(words)
        self.assertIn('来る', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'とか')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
