import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nikotaeru.index import RULE, match_nikotaeru
from src.grammar_rules.rules.nikotaete.index import match_nikotaete
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NikotaeruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'期待に応える判断。': 'に応える',
                 '期待に応えた計画。': 'に応えた',
                 'それに応える。': 'に応える',
                 '要求にこたえた計画。': 'にこたえた',
                 '期待に応えて判断する。': None,
                 '応える判断。': None, '要求に 応える。': None,
                 '質問に答える。': None,
                 '期待に応えます。': None, '期待に応えない。': None,
                 '期待に応じる。': None, '期待を応える。': None,
                 '期待に、応える。': None, '期待 に応える。': None,
                 '暑さが身にこたえる。': 'にこたえる',
                 '期待に応える人。': 'に応える',
                 '期待に応えた。': 'に応えた',
                 '期待に応えるなら頑張ろう。': 'に応える'}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nikotaeru(tokens)],
                                 [] if expected is None else [expected])
                if '応えて' in text:
                    self.assertEqual([text[a:b] for a, b in match_nikotaete(tokens)], ['に応えて'])

    def test_reservation_and_identity(self):
        source = sentence('期待に応えた計画を発表しました。', [
            tok('期待', '期待', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('応え', '応える', ('動詞',)), tok('た', 'た', ('助動詞',)),
            tok('計画を発表しました。', '計画', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '応え', 'た'])
        words = {'期待': object()}
        stage.add_cards(words)
        self.assertIn('期待', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に応えた')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
