import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nishitatokorode.index import RULE
from src.grammar_rules.rules.tatokorode.index import RULE as TATOKORODE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NishitatokorodeTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨にしたところで出かける。': True,
            '行くにしたところで時間がない。': True,
            '彼にした所で同じだ。': True,
            '仕事をしたところで終わらない。': False,
            '彼としたところで同じだ。': False,
            '彼にしたところへ行く。': False,
            '彼に したところで同じだ。': False,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual(bool(list(RULE.match(tokens))), expected)

    def test_identity_reservation_and_overlap(self):
        source = sentence('雨にしたところで出かける。', [
            tok('雨', '雨', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('し', 'する', ('動詞',)), tok('た', 'た', ('助動詞',)),
            tok('ところ', 'ところ', ('名詞',)), tok('で', 'で', ('助詞',)),
            tok('出かける', '出かける', ('動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, TATOKORODE))
        data = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'し', 'た', 'ところ', 'で'])
        words = {'雨': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'にしたところで')
        self.assertIn(TATOKORODE.expression, words)
        self.assertIn('雨', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
