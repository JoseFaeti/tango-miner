import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.bakaridenaku.index import RULE, match_bakaridenaku
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class BakaridenakuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'本ばかりでなく雑誌も読む。': 'ばかりでなく',
                 '読むばかりではなく書く。': 'ばかりではなく',
                 '安いばかりじゃなく便利だ。': 'ばかりじゃなく',
                 '静かなばかりでなく便利だ。': 'ばかりでなく',
                 '本ばかりではない。': None, '本ばかり読む。': None,
                 '本ばかりで なく雑誌も読む。': None, '本 ばかりでなく雑誌も読む。': None,
                 '本ばかりでなくなる。': None,
                 '読んだばかりでなく書いた。': 'ばかりでなく'}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_bakaridenaku(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity_with_competing_rules(self):
        source = sentence('本ばかりでなく雑誌も読む。', [
            tok('本', '本', ('名詞',)), tok('ばかり', 'ばかり', ('助詞', '副助詞')),
            tok('で', 'だ', ('助動詞',)), tok('なく', 'ない', ('形容詞',)),
            tok('雑誌も読む。', '雑誌', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['ばかり', 'で', 'なく'])
        words = {'本': object()}
        stage.add_cards(words)
        self.assertIn('本', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ばかりでなく')
        self.assertNotIn('〜ばかり', words)
        self.assertNotIn('〜たばかり', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)

    def test_past_additive_does_not_emit_recent_card(self):
        source = sentence('読んだばかりでなく書いた。', [
            tok('読ん', '読む', ('動詞',)), tok('だ', 'だ', ('助動詞',)),
            tok('ばかり', 'ばかり', ('助詞', '副助詞')),
            tok('で', 'だ', ('助動詞',)), tok('なく', 'ない', ('形容詞',)),
            tok('書いた。', '書く', ('動詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source]))
        words = {}
        stage.add_cards(words)
        self.assertIn(RULE.expression, words)
        self.assertNotIn('〜たばかり', words)
        self.assertNotIn('〜ばかり', words)
