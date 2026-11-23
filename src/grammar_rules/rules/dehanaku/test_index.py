import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.dehanaku.index import RULE, match_dehanaku
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class DehanakuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'猫ではなく犬だ。': 'ではなく', '学生じゃなく先生だ。': 'じゃなく',
                 '静かではなく賑やかだ。': 'ではなく', '読むのではなく書く。': 'ではなく',
                 '学生ではない。': None, '学校ではなくなる。': None,
                 '学生ではなくて困った。': None, '猫 ではなく犬だ。': None,
                 '猫では なく犬だ。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_dehanaku(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_specific_precedence(self):
        source = sentence('これは猫ではなく犬だと思う。', [
            tok('これは', 'これ', ('代名詞',)),
            tok('猫', '猫', ('名詞',)), tok('で', 'で', ('助詞', '格助詞')),
            tok('は', 'は', ('助詞',)), tok('なく', 'ない', ('形容詞',)),
            tok('犬だと思う。', '犬', ('名詞',))])
        stage = GrammarStep()
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['で', 'は', 'なく'])
        words = {'猫': object()}
        stage.add_cards(words)
        self.assertIn('猫', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ではなく')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)

        source = sentence('本だけではなく雑誌も読む。', [
            tok('本', '本', ('名詞',)), tok('だけ', 'だけ', ('助詞', '副助詞')),
            tok('で', 'だ', ('助動詞',)), tok('は', 'は', ('助詞',)),
            tok('なく', 'ない', ('形容詞',)), tok('雑誌も読む。', '雑誌', ('名詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source]))
        words = {}
        stage.add_cards(words)
        self.assertIn('〜だけでなく', words)
        self.assertIn(RULE.expression, words)
