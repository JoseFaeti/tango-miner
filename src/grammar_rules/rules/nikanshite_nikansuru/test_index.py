import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nikanshite_nikansuru.index import RULE, match_nikanshite_nikansuru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NikanshiteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'問題に関して話す。': 'に関して', 'それに関しては知らない。': 'に関して',
                 '問題に関しましてお知らせします。': 'に関しまして',
                 '環境に関する本を読む。': 'に関する', '問題に関しての説明。': 'に関して',
                 '事件に関係する人。': None, '彼が関する問題。': None,
                 '問題に 関して話す。': None, '関心がある。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nikanshite_nikansuru(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('この問題に関して詳しく話しましょう。', [
            tok('この', 'この', ('連体詞',)),
            tok('問題', '問題', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('関し', '関する', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('詳しく話しましょう。', '詳しい', ('形容詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '関し', 'て'])
        words = {'問題': object()}
        stage.add_cards(words)
        self.assertIn('問題', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に関して')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
