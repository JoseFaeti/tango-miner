import unittest

from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import tok
from src.grammar_rules.rules.te_iru.test_index import sentence
from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.rules.omonotomosezu.index import RULE


class OmonotomosezuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        from sudachipy import dictionary, tokenizer
        from src.grammar_rules.rules.omonotomosezu.index import match_omonotomosezu
        analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        cases = {
            '困難をものともせず進んだ。': 'をものともせず',
            '雨を物ともせずに走った。': 'を物ともせずに',
            '困難をものともしない。': None,
            '物ともせずに進んだ。': None,
            '本をものともせずに置いた。': 'をものともせずに',
            '困難を ものともせず進んだ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_omonotomosezu(tokens)],
                                 [] if expected is None else [expected], raw)

    def test_reservation_identity_and_attached_word(self):
        source = sentence('困難をものともせず進んだ。', [
            tok('困難', '困難', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('もの', 'もの', ('名詞',)), tok('と', 'と', ('助詞',)),
            tok('も', 'も', ('助詞',)), tok('せ', 'する', ('動詞',)),
            tok('ず', 'ず', ('助動詞',)), tok('進んだ。', '進む', ('動詞',))])
        stage = GrammarStep()
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['を', 'もの', 'と', 'も', 'せ', 'ず'])
        words = {'困難': object()}
        stage.add_cards(words)
        self.assertIn('困難', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'をものともせず')
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
