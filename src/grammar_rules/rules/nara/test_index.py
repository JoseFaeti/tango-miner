import unittest
from unittest.mock import patch
from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.nara.index import RULE, match_nara
from src.grammar_rules.rules.te_iru.test_index import sentence, tok

class NaraTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'雨が降るなら帰る。': True, '行ったなら話す。': True,
                 '寒いなら帰る。': True, '静かなら行く。': True,
                 '学生ならきっと分かるはずだ。': True, '彼ならできる。': True,
                 '行かないなら帰る。': True, '行くのなら帰る。': True,
                 '奈良へ行く。': False, 'そうならない。': False,
                 '行くなら。': False, '行く なら帰る。': False}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nara(aligned)],
                                 ['なら'] if expected else [])

    def test_reservation_and_expression_precedence(self):
        def source():
            return sentence('学生ならきっと分かるはずだ。', [
                tok('学生', '学生', ('名詞',)),
                tok('なら', 'だ', ('助動詞', '*', '*', '*', '助動詞-ダ', '仮定形-一般')),
                tok('きっと分かるはずだ。', '分かる', ('動詞',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source()])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')], ['なら'])
        words = {'学生': object(), '分かる': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'なら')
        self.assertIn('学生', words)
        self.assertIn('分かる', words)
        item = source()
        merged = [dict(surface='学生なら', lemma='学生なら', pos=('exp',),
                       analysis_rule='jmdict_surface_merge'), item.tokens[-1]]
        stage = GrammarStep(rules=(RULE,), prefer_expressions=True)
        with patch('src.steps.MergeTokensStep.get_jmdict'), patch('src.steps.MergeTokensStep.combine_tokens_jmdict', return_value=merged):
            result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([item])))
        self.assertEqual(stage.occurrences[RULE.id].count, 0)
        self.assertFalse(any(t.get('grammar_reserved') for t in result.data[0].tokens))
