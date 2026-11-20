import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.naide.index import RULE as SHORT_RULE
from src.grammar_rules.rules.naidehasumanai.index import RULE, match_naidehasumanai


class NaidehasumanaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '謝らないでは済まない。': 'ないでは済まない',
            '説明しないではすまなかった。': 'ないではすまなかった',
            '謝らないでは済みません。': 'ないでは済みません',
            '謝らないでは済まなければ。': 'ないでは済まなければ',
            '説明させないでは済まない。': 'ないでは済まない',
            '謝らないで済む。': None, '謝らないでは済む。': None,
            '謝らないでは住まない。': None, '謝らないでは帰らない。': None,
            'ないでは済まない。': None, '謝らないで は済まない。': None,
            '謝らないでは済まなかっ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_naidehasumanai(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_overlap_and_example(self):
        parts = [tok('謝ら', '謝る', ('動詞', '一般', '*', '*', '*', '未然形-一般')),
                 tok('ない', 'ない', ('助動詞',)), tok('で', 'で', ('助詞', '接続助詞')),
                 tok('は', 'は', ('助詞',)), tok('済ま', '済む', ('動詞',)),
                 tok('ない', 'ない', ('助動詞',)), tok('。', '。', ('補助記号',))]
        stage = GrammarStep(rules=(RULE, SHORT_RULE))
        result = ExcludeGrammarTokensStep(stage).process(
            stage.process(Artifact([sentence('謝らないでは済まない。', parts)])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ない', 'で', 'は', '済ま', 'ない'])
        words = {'謝る': object()}
        stage.add_cards(words)
        self.assertIn('謝る', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ないでは済まない')
        self.assertIn(SHORT_RULE.expression, words)
