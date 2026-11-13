import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.toshitemo.index import RULE, match_toshitemo
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class ToshitemoTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'雨が降るとしても出かける。': True,
                 '高いとしても買いたい。': True,
                 '彼が犯人だとしても驚かない。': True,
                 '代表としても働く。': True,
                 '帰ろうとしても帰れない。': True,
                 '仕事をしても終わらない。': False,
                 '教師として働く。': False,
                 '雨だと しても出かける。': False}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toshitemo(tokens)],
                                 ['としても'] if expected else [])

    def test_reservation_and_identity(self):
        source = sentence('雨だとしても出かける。', [
            tok('雨', '雨', ('名詞',)), tok('だ', 'だ', ('助動詞',)),
            tok('と', 'と', ('助詞',)), tok('し', 'する', ('動詞',)),
            tok('て', 'て', ('助詞',)), tok('も', 'も', ('助詞',)),
            tok('出かける', '出かける', ('動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        artifact = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens if t.get('grammar_reserved')],
                         ['だ', 'と', 'し', 'て', 'も'])
        words = {'雨': object()}
        stage.add_cards(words)
        self.assertIn('雨', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'としても')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
