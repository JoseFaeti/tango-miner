import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.otowazu_otowanai.index import RULE, match_otowazu_otowanai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class OtowazuOtowanaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '年齢を問わず参加できる。': 'を問わず',
            '経験を問わない仕事。': 'を問わない',
            '国籍をとわない。': 'をとわない',
            '責任を問わずに済ませた。': None,
            '問わない。': None,
            '年齢を 問わない。': None,
            '経験を問わなかった。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_otowazu_otowanai(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('経験を問わない仕事。', [
            tok('経験', '経験', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('問わ', '問う', ('動詞',)), tok('ない', 'ない', ('助動詞',)),
            tok('仕事', '仕事', ('名詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '問わ', 'ない'])
        words = {'経験': object()}
        stage.add_cards(words)
        self.assertIn('経験', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を問わない')
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
