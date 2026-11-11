import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.te_itadakemasenka.index import RULE, match_te_itadakemasenka


class TeItadakemasenkaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '食べていただけませんか。': 'ていただけませんか',
            '読んで頂けませんか？': 'で頂けませんか',
            '見せていただけませんかね。': 'ていただけませんか',
            '読んでいただけますか。': None, '読んでいただけません。': None,
            '本をいただけませんか。': None, '雨でいただけませんか。': None,
            '読んで いただけませんか。': None, '読んでいただく。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_te_itadakemasenka(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_card(self):
        verb = ('動詞', '一般', '*', '*', '下一段', '連用形-一般')
        auxiliary = ('動詞', '非自立可能', '*', '*', '下一段', '連用形-一般')
        source = sentence('これを食べていただけませんか。', [
            tok('これを', 'これ', ('名詞',)), tok('食べ', '食べる', verb),
            tok('て', 'て', ('助詞', '接続助詞')), tok('いただけ', 'いただける', auxiliary),
            tok('ませ', 'ます', ('助動詞',)), tok('ん', 'ぬ', ('助動詞',)),
            tok('か', 'か', ('助詞', '終助詞')), tok('。', '。', ('補助記号',))])
        self.assertIn(RULE, RULES)
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source, source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['て', 'いただけ', 'ませ', 'ん', 'か'])
        words = {'食べる': object()}
        stage.add_cards(words)
        self.assertIn('食べる', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ていただけませんか')


if __name__ == '__main__':
    unittest.main()
