import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.tohahantaini.index import RULE, match_tohahantaini
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TohahantainiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '予想とは反対に晴れた。': 'とは反対に',
            '思っていたこととは反対に進んだ。': 'とは反対に',
            '期待していたとは反対に失敗した。': 'とは反対に',
            '兄とは反対に弟は静かだ。': 'とは反対に',
            '予想と反対に進んだ。': None,
            '予想とは反対だ。': None,
            '反対に進んだ。': None,
            '予想とは反対の結果だ。': None,
            '予想とは 反対に進んだ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tohahantaini(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('予想とは反対に晴れた。', [
            tok('予想', '予想', ('名詞',)), tok('と', 'と', ('助詞',)),
            tok('は', 'は', ('助詞',)), tok('反対', '反対', ('名詞',)),
            tok('に', 'に', ('助詞',)), tok('晴れた。', '晴れる', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['と', 'は', '反対', 'に'])
        words = {'予想': object()}
        stage.add_cards(words)
        self.assertIn('予想', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'とは反対に')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
