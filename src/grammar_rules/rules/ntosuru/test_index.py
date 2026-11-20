import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.ntosuru.index import RULE, match_ntosuru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NtosuruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '本を読まんとする。': ['んとする'], '家を出んとした。': ['んとした'],
            '来んとしている。': ['んとして'], '勉強せんとする。': ['んとする'],
            '帰らんとしない。': ['んとしない'], '行かんとしました。': ['んとしました'],
            '本を読むとする。': [], 'なんとする。': [], '読まんと思う。': [],
            '読まんと する。': [], '読んとする。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ntosuru(tokens)], expected, raw)

    def test_reservation_identity_and_lexical_verb(self):
        sources = [sentence('私は本を読まんとした。', [
            tok('私は本を', '本', ('名詞',)),
            tok('読ま', '読む', ('動詞', '一般', '*', '*', '五段-マ行', '未然形-一般')),
            tok('ん', 'ぬ', ('助動詞',)), tok('と', 'と', ('助詞',)),
            tok('し', 'する', ('動詞',)), tok('た', 'た', ('助動詞',)),
            tok('。', '。', ('補助記号',))]),
            sentence('私は食事を食べんとする。', [
                tok('私は食事を', '食事', ('名詞',)),
                tok('食べん', '食べる', ('動詞', '一般', '*', '*', '下一段-バ行', '連体形-撥音便')),
                tok('と', 'と', ('助詞',)), tok('する', 'する', ('動詞',)),
                tok('。', '。', ('補助記号',))])]
        stage = GrammarStep()
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact(sources)))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ん', 'と', 'し', 'た'])
        self.assertFalse(result.data[1].tokens[1].get('grammar_reserved'))
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertNotIn('〜ぬ / 〜ん', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)

    def test_incomplete_suffix(self):
        parts = [tok('読ま', '読む', ('動詞', '一般', '*', '*', '五段-マ行', '未然形-一般')),
                 tok('ん', 'ぬ', ('助動詞',)), tok('と', 'と', ('助詞',)),
                 tok('し', 'する', ('動詞',))]
        text = ''.join(t['surface'] for t in parts)
        tokens, error = aligned_tokens(text, parts)
        self.assertIsNone(error)
        self.assertEqual(list(match_ntosuru(tokens)), [])
