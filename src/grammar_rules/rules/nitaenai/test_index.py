import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nitaenai.index import RULE, match_nitaenai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NitaenaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '見るに堪えない。': 'に堪えない',
            '感謝に堪えません。': 'に堪えません',
            '感謝に堪えませんでした。': 'に堪えませんでした',
            '悲しみにたえなかった。': 'にたえなかった',
            '見るに耐えなかった。': 'に耐えなかった',
            '観賞に堪えない。': 'に堪えない',
            '寒さに耐えない。': 'に耐えない',
            '苦痛に堪える。': None,
            '痛みを堪えない。': None,
            '見るに 堪えない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nitaenai(tokens)],
                                 [] if expected is None else [expected])

    def test_incomplete_suffix_and_nonverb(self):
        for verb, suffix in [
            (tok('堪え', '堪える', ('動詞',)), tok('なかっ', 'ない', ('助動詞',))),
            (tok('堪え', '堪える', ('名詞',)), tok('ない', 'ない', ('助動詞',))),
        ]:
            raw = [tok('感謝', '感謝', ('名詞',)), tok('に', 'に', ('助詞',)), verb, suffix]
            text = ''.join(token['surface'] for token in raw)
            tokens, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_nitaenai(tokens)), [])

    def test_reservation_and_identity(self):
        source = sentence('見るに堪えない光景だった。', [
            tok('見る', '見る', ('動詞',)), tok('に', 'に', ('助詞',)),
            tok('堪え', '堪える', ('動詞',)), tok('ない', 'ない', ('助動詞',)),
            tok('光景だった。', '光景', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '堪え', 'ない'])
        words = {'見る': object()}
        stage.add_cards(words)
        self.assertIn('見る', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に堪えない')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)


if __name__ == '__main__':
    unittest.main()
