import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.kara_node.index import RULE as REASON_RULE
from src.grammar_rules.rules.no.index import RULE, match_no
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NoTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'私の本。': 1, '日本の文化。': 1, '私の。': 1,
                 '読むのが好き。': 1, '赤いのを買う。': 1,
                 '猫の目の色。': 2, 'のどが痛い。': 0, 'その本。': 0,
                 '本 の表紙。': 0}
        for text, count in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_no(tokens)], ['の'] * count)

    def test_reservation_and_identity(self):
        source = sentence('私の本を読むのが好きです。', [
            tok('私', '私', ('代名詞',)), tok('の', 'の', ('助詞', '格助詞')),
            tok('本', '本', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('読む', '読む', ('動詞',)), tok('の', 'の', ('助詞', '準体助詞')),
            tok('が', 'が', ('助詞',)), tok('好き', '好き', ('形状詞',)),
            tok('です', 'です', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')], ['の', 'の'])
        words = {'本': object(), '読む': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'の')
        self.assertTrue(all(word in words for word in ('本', '読む')))
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)

    def test_controls(self):
        for previous, particle in [
            (tok('本', '本', ('名詞',)), tok('の', 'の', ('助詞', '終助詞'))),
            (tok('、', '、', ('補助記号', '読点')), tok('の', 'の', ('助詞', '格助詞'))),
            (tok('本', '本', ('名詞',)), tok('の', '違う', ('助詞', '格助詞'))),
        ]:
            tokens, error = aligned_tokens(previous['surface'] + particle['surface'], [previous, particle])
            self.assertIsNone(error)
            self.assertEqual(list(match_no(tokens)), [])

    def test_overlap_with_reason(self):
        source = sentence('読むので今日は休みます。', [
            tok('読む', '読む', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('の', 'の', ('助詞', '準体助詞')),
            tok('で', 'だ', ('助動詞', '*', '*', '*', '助動詞-ダ', '連用形-一般')),
            tok('今日は休みます。', '今日', ('名詞',))])
        stage = GrammarStep(rules=(RULE, REASON_RULE))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        self.assertEqual(stage.occurrences[REASON_RULE.id].count, 1)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['の', 'で'])
