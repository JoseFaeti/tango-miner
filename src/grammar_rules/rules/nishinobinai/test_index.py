import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nishinobinai.index import RULE, match_nishinobinai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NishinobinaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '見るに忍びない。': 'に忍びない',
            '捨てるに忍びなかった。': 'に忍びなかった',
            '見るに忍びません。': 'に忍びません',
            '見るに忍びませんでした。': 'に忍びませんでした',
            '見るにしのびない。': 'にしのびない',
            '寒さに忍びない。': 'に忍びない',
            '敵に忍び寄る。': None,
            '痛みを忍びない。': None,
            '見るに 忍びない。': None,
            '見るに忍ぶ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nishinobinai(tokens)],
                                 [] if expected is None else [expected])

    def test_incomplete_and_nonverb(self):
        for verb, suffix in [
            (tok('忍び', '忍ぶ', ('動詞',)), tok('なかっ', 'ない', ('助動詞',))),
            (tok('忍び', '忍ぶ', ('名詞',)), tok('ない', 'ない', ('助動詞',))),
        ]:
            raw = [tok('見る', '見る', ('動詞',)), tok('に', 'に', ('助詞',)), verb, suffix]
            text = ''.join(token['surface'] for token in raw)
            tokens, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_nishinobinai(tokens)), [])

    def test_reservation_and_identity(self):
        source = sentence('見るに忍びない光景だった。', [
            tok('見る', '見る', ('動詞',)), tok('に', 'に', ('助詞',)),
            tok('忍び', '忍ぶ', ('動詞',)), tok('ない', 'ない', ('助動詞',)),
            tok('光景だった。', '光景', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '忍び', 'ない'])
        words = {'見る': object()}
        stage.add_cards(words)
        self.assertIn('見る', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に忍びない')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
