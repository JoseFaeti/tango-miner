import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.kagirino.index import RULE, match_kagirino
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KagirinoTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '持てる限りの力を尽くす。': '限りの',
            '思いつくかぎりの言葉を並べた。': 'かぎりの',
            '命の限りの努力をする。': '限りの',
            'できる限りのことをする。': None,
            '知る限りでは本当だ。': None,
            '生きる限り希望はある。': None,
            '限りのある資源を守る。': None,
            '持てる 限りの力を尽くす。': None,
            '持てる限り の力を尽くす。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kagirino(tokens)],
                                 [] if expected is None else [expected], tokens)

    def test_reservation_and_identity(self):
        source = sentence('持てる限りの力を尽くす。', [
            tok('持てる', '持てる', ('動詞', '一般', '*', '*', '下一段', '連体形-一般')),
            tok('限り', '限り', ('名詞',)), tok('の', 'の', ('助詞',)),
            tok('力を尽くす。', '力', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['限り', 'の'])
        words = {'持てる': object()}
        stage.add_cards(words)
        self.assertIn('持てる', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, '限りの')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(stage.occurrences['kagiri'].count, 0)
