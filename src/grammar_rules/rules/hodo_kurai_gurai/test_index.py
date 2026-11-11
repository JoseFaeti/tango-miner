import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.hodo_kurai_gurai.index import RULE, match_hodo_kurai_gurai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class HodoKuraiGuraiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'死ぬほど疲れた。': ['ほど'], '泣く程うれしい。': ['程'],
                 '三日くらい待つ。': ['くらい'], 'これぐらい大丈夫。': ['ぐらい'],
                 'どれくらいかかる？': ['くらい'], '食べたいぐらいだ。': ['ぐらい'],
                 'それほどでもない。': ['ほど'], '暗い部屋。': [], '位が高い。': [],
                 'この程度のこと。': [], '本 くらい読む。': []}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_hodo_kurai_gurai(tokens)], expected)

    def test_reservation_identity_and_lexical_evidence(self):
        source = sentence('今日は死ぬほど疲れた。', [
            tok('今日', '今日', ('名詞',)), tok('は', 'は', ('助詞',)),
            tok('死ぬ', '死ぬ', ('動詞',)), tok('ほど', 'ほど', ('助詞', '副助詞')),
            tok('疲れた。', '疲れる', ('動詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')], ['ほど'])
        lexical = object()
        words = {'死ぬ': object(), 'ほど': lexical}
        stage.add_cards(words)
        self.assertIs(words['ほど'], lexical)
        self.assertIn('死ぬ', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ほど')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
