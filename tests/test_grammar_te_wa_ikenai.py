import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarShadow import aligned_tokens
from src.grammar_rules.rules.te_wa_ikenai import RULE, match_te_wa_ikenai
from tests.test_grammar_te_iru import sentence, tok


class TeWaIkenaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'食べてはいけない。': 'てはいけない', '行ってはいけません。': 'てはいけません',
                 '読んじゃいけない。': 'じゃいけない', '食べちゃいけなかった。': 'ちゃいけなかった',
                 '行っちゃいけませんでした。': 'ちゃいけませんでした',
                 '雨じゃいけない。': None, '帰らなきゃいけない。': None,
                 '行っては いけない。': None, '行ってはいけなければ。': None,
                 '行ってはいけませんでし。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_te_wa_ikenai(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_retains_verb_and_control(self):
        verb = ('動詞', '一般', '*', '*', '下一段', '連用形-一般')
        matched = sentence('今は食べてはいけないですよ。', [tok('今は', '今', ('名詞',)), tok('食べ', '食べる', verb),
            tok('ては', 'ては', ('助詞', '接続助詞')), tok('いけ', 'いける', ('動詞',)),
            tok('ない', 'ない', ('助動詞',)), tok('ですよ。', 'です', ('助動詞',))])
        control = sentence('雨じゃいけない。', [tok('雨', '雨', ('名詞',)),
            tok('じゃ', 'じゃ', ('助動詞',)), tok('いけない', 'いけない', ('形容詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([matched, control])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')], ['ては', 'いけ', 'ない'])
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved'))
        self.assertFalse(any(t.get('grammar_reserved') for t in result.data[1].tokens))
        self.assertEqual(stage.occurrences[RULE.id].count, 1)


if __name__ == '__main__':
    unittest.main()
