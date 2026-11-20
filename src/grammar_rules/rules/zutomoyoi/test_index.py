import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.zutomo.index import RULE as ZUTOMO
from src.grammar_rules.rules.zutomoyoi.index import RULE, match_zutomoyoi


class ZutomoyoiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'行かずともよい。': 'ずともよい', '言わずとも良い。': 'ずとも良い',
                 'せずともいい。': 'ずともいい', '行かずともよかった。': 'ずともよかった',
                 '食べられずともよい。': 'ずともよい', '言わずとも分かる。': None,
                 '行かずともよくない。': None, '行かずとも よい。': None,
                 '行か ずともよい。': None, 'ずともよい。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_zutomoyoi(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_overlap_and_identity(self):
        source = sentence('今日は言わずとも良いはずだ。', [
            tok('今日は', '今日', ('名詞',)),
            tok('言わ', '言う', ('動詞', '一般', '*', '*', '五段', '未然形-一般')),
            tok('ず', 'ず', ('助動詞',)), tok('とも', 'とも', ('助詞',)),
            tok('良い', '良い', ('形容詞',)), tok('はずだ', 'はずだ', ('名詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(ZUTOMO, RULE))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ず', 'とも', '良い'])
        self.assertEqual(stage.occurrences[ZUTOMO.id].count, 1)
        words = {'言う': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ずとも良い')
        self.assertIn('言う', words)
