import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.ka.index import RULE, match_ka
from src.grammar_rules.rules.noka_nka.index import RULE as NOKA
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'行きますか。': 1, '誰ですか？': 1, '行くか。': 1,
                 '行くのか。': 1, '蚊がいる。': 0, 'かばんを買う。': 0,
                 '行く か。': 0}
        for text, count in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ka(tokens)], ['か'] * count)

    def test_reservation_identity_and_overlap(self):
        source = sentence('あなたは本を読むのか。', [
            tok('あなた', 'あなた', ('代名詞',)), tok('は', 'は', ('助詞', '係助詞')),
            tok('本', '本', ('名詞', '普通名詞', '一般')),
            tok('を', 'を', ('助詞', '格助詞')),
            tok('読む', '読む', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('の', 'の', ('助詞', '準体助詞')),
            tok('か', 'か', ('助詞', '終助詞')),
            tok('。', '。', ('補助記号', '句点'))])
        stage = GrammarStep(rules=(RULE, NOKA))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['の', 'か'])
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        self.assertEqual(stage.occurrences[NOKA.id].count, 1)
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'か')
        self.assertIn('読む', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
