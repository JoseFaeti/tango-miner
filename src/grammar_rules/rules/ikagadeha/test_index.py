import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.ikagadeha.index import RULE, match_ikagadeha
from src.grammar_rules.rules.ikande_ikanniyotteha.index import match_ikande
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class IkagadehaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '結果如何では中止する。': '如何では',
            '結果のいかんでは中止する。': 'いかんでは',
            '結果如何で決める。': None,
            '結果如何によっては中止する。': None,
            'いかんではない。': None,
            '如何ではございますか。': None,
            '結果の いかんでは中止する。': None,
            '結果如何で は中止する。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ikagadeha(tokens)],
                                 [] if expected is None else [expected])
                if expected:
                    self.assertEqual(list(match_ikande(tokens)), [])

    def test_reservation_and_identity(self):
        source = sentence('結果のいかんでは中止する。', [
            tok('結果', '結果', ('名詞',)), tok('の', 'の', ('助詞',)),
            tok('いかん', 'いかん', ('名詞',)), tok('で', 'で', ('助詞',)),
            tok('は', 'は', ('助詞',)), tok('中止する。', '中止', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['いかん', 'で', 'は'])
        words = {'結果': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'いかんでは')
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertIn('結果', words)
