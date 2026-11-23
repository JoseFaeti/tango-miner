import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nikagitsutakotodehanai.index import (
    RULE,
    match_nikagitsutakotodehanai,
)
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NikagitsutakotodehanaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '子供に限ったことではない。': 'に限ったことではない',
            '日本に限ったことじゃない。': 'に限ったことじゃない',
            'これにかぎったことではありません。': 'にかぎったことではありません',
            '彼に限ったことではなかった。': 'に限ったことではなかった',
            'これに限った事ではない。': 'に限った事ではない',
            '子供に限ったことではありませんでした。': 'に限ったことではありませんでした',
            '子供に限ったことだ。': None,
            '子供に限ったことでもない。': None,
            '時間を限ったことではない。': None,
            '子供に 限ったことではない。': None,
            '子供に限ったことではある。': None,
            '子供に限ったことではなかっ': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nikagitsutakotodehanai(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('子供に限ったことではない。', [
            tok('子供', '子供', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('限っ', '限る', ('動詞',)), tok('た', 'た', ('助動詞',)),
            tok('こと', 'こと', ('名詞',)), tok('で', 'で', ('助詞',)),
            tok('は', 'は', ('助詞',)), tok('ない', 'ない', ('形容詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '限っ', 'た', 'こと', 'で', 'は', 'ない'])
        words = {'子供': object()}
        stage.add_cards(words)
        self.assertIn('子供', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に限ったことではない')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
