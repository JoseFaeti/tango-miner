import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.zuni.index import RULE as SHORT_RULE
from src.grammar_rules.rules.zunihasumanai.index import RULE, match_zunihasumanai


class ZunihasumanaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '謝らずには済まない。': 'ずには済まない',
            '説明せずにはすまなかった。': 'ずにはすまなかった',
            '謝らずには済みません。': 'ずには済みません',
            '謝らずには済まなければ。': 'ずには済まなければ',
            '説明させずには済まない。': 'ずには済まない',
            '謝らずに済む。': None, '謝らずには済む。': None,
            '謝らずには住まない。': None, '謝らずには帰らない。': None,
            'ずには済まない。': None, '謝らずに は済まない。': None,
            '謝らずには済まなかっ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_zunihasumanai(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_overlap_and_example(self):
        parts = [tok('謝ら', '謝る', ('動詞', '一般', '*', '*', '*', '未然形-一般')),
                 tok('ず', 'ず', ('助動詞',)), tok('に', 'に', ('助詞',)),
                 tok('は', 'は', ('助詞',)), tok('済ま', '済む', ('動詞',)),
                 tok('ない', 'ない', ('助動詞',)), tok('。', '。', ('補助記号',))]
        stage = GrammarStep(rules=(RULE, SHORT_RULE))
        result = ExcludeGrammarTokensStep(stage).process(
            stage.process(Artifact([sentence('謝らずには済まない。', parts)])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ず', 'に', 'は', '済ま', 'ない'])
        words = {'謝る': object()}
        stage.add_cards(words)
        self.assertIn('謝る', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ずには済まない')
        self.assertIn(SHORT_RULE.expression, words)
