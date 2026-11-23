import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.tomokagiranai.index import RULE, match_tomokagiranai
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class TomokagiranaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            'そうとも限らない。': 'とも限らない',
            '成功するとも限らなかった。': 'とも限らなかった',
            '正しいともかぎりませんでした。': 'ともかぎりませんでした',
            'そうとも限らなくてもよい。': 'とも限らなく',
            '雨が降らないとも限らない。': None,
            'そうとは限らない。': None,
            'そうとも限る。': None,
            'そうと も限らない。': None,
            'そうとも限らぬ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tomokagiranai(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_negative_construction(self):
        sources = [sentence('いつもそうとも限らない。', [
            tok('いつも', 'いつも', ('副詞',)), tok('そう', 'そう', ('副詞',)),
            tok('と', 'と', ('助詞', '格助詞')),
            tok('も', 'も', ('助詞', '係助詞')), tok('限ら', '限る', ('動詞',)),
            tok('ない', 'ない', ('助動詞',)), tok('。', '。', ('補助記号',))]),
            sentence('降らないとも限らない。', [
                tok('降ら', '降る', ('動詞',)), tok('ない', 'ない', ('助動詞',)),
                tok('と', 'と', ('助詞', '格助詞')), tok('も', 'も', ('助詞', '係助詞')),
                tok('限ら', '限る', ('動詞',)), tok('ない', 'ない', ('助動詞',)),
                tok('。', '。', ('補助記号',))])]
        stage = GrammarStep()
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact(sources)))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['と', 'も', '限ら', 'ない'])
        words = {'降る': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'とも限らない')
        self.assertEqual(len(words[RULE.expression].sentences), 1)
        self.assertIn('〜ないとも限らない', words)
        self.assertIn('降る', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
