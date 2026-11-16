import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.nitaeru.index import RULE, match_nitaeru
from src.grammar_rules.rules.nitaenai.index import match_nitaenai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NitaeruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '鑑賞に堪える作品だ。': 'に堪える',
            '見るに堪える作品だ。': 'に堪える',
            '風雨に耐えた。': 'に耐えた',
            '使用に耐えます。': 'に耐えます',
            '使用に耐えました。': 'に耐えました',
            '苦痛にたえて生きる。': 'にたえて',
            '重さに耐えればよい。': 'に耐えれば',
            '見るに堪えない。': None,
            '感謝に堪えません。': None,
            '痛みを堪える。': None,
            '苦痛に 堪える。': None,
            '期待に応える。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nitaeru(tokens)],
                                 [] if expected is None else [expected])
                if expected:
                    self.assertEqual(list(match_nitaenai(tokens)), [])

    def test_incomplete_and_nonverb(self):
        for verb in [tok('堪え', '堪える', ('動詞',)), tok('堪える', '堪える', ('名詞',))]:
            raw = [tok('鑑賞', '鑑賞', ('名詞',)), tok('に', 'に', ('助詞',)), verb]
            text = ''.join(t['surface'] for t in raw)
            tokens, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_nitaeru(tokens)), [])

    def test_reservation_and_identity(self):
        source = sentence('見るに堪える作品だった。', [
            tok('見る', '見る', ('動詞',)), tok('に', 'に', ('助詞',)),
            tok('堪える', '堪える', ('動詞',)), tok('作品だった。', '作品', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '堪える'])
        words = {'見る': object()}
        stage.add_cards(words)
        self.assertIn('見る', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に堪える')
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
