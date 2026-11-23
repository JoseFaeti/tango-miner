import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.ohete.index import RULE, match_ohete
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class OheteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '議論を経て決定した。': 'を経て', '十年を経て帰った。': 'を経て',
            '東京を経て大阪へ行く。': 'を経て', '審査を経まして決定しました。': 'を経まして',
            '議論を経た結果。': None, '時が経てばわかる。': None,
            '絵を描いて見せた。': None, '議論を 経て決定した。': None,
            '議論を経まし て決定した。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ohete(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('議論を経て決定した。', [
            tok('議論', '議論', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('経', '経る', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('決定した。', '決定', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '経', 'て'])
        words = {'議論': object()}
        stage.add_cards(words)
        self.assertIn('議論', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を経て')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
