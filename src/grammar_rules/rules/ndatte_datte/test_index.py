import unittest
from unittest.mock import patch

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.ndatte_datte.index import RULE, match_ndatte_datte
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NdatteDatteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'行くんだって。': 'んだって', '学生なんだって。': 'んだって',
                 '雨だって。': 'だって', '静かだって。': 'だって',
                 '読んだって。': None, 'だって無理。': None,
                 '猫だって好き。': None, '行くんだって言った。': None,
                 '何だって？': None, '行くんだ って。': None,
                 '猫だって、好き。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ndatte_datte(aligned)],
                                 [expected] if expected else [], raw)

    def test_reservation_identity_and_components(self):
        source = sentence('明日は学校に行くんだって。', [
            tok('明日は学校に', '学校', ('名詞',)), tok('行く', '行く', ('動詞',)),
            tok('ん', 'ん', ('助詞', '準体助詞')), tok('だ', 'だ', ('助動詞',)),
            tok('って', 'って', ('助詞', '副助詞')), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['ん', 'だ', 'って'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'んだって')
        self.assertIn('行く', words)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertIn(RULE, RULES)

    def test_copular_na_survives_dictionary_precedence(self):
        source = sentence('彼は学生なんだって。', [
            tok('彼は学生', '学生', ('名詞',)), tok('な', 'だ', ('助動詞',)),
            tok('ん', 'ん', ('助詞', '準体助詞')), tok('だ', 'だ', ('助動詞',)),
            tok('って', 'って', ('助詞', '副助詞')), tok('。', '。', ('補助記号',))])
        merged = [source.tokens[0], {'surface': 'なんだって', 'lemma': 'なんだって',
                  'pos': ('exp',), 'analysis_rule': 'jmdict_surface_merge'}, source.tokens[-1]]
        stage = GrammarStep(rules=(RULE,), prefer_expressions=True)
        with patch('src.steps.MergeTokensStep.index.get_jmdict'), \
             patch('src.steps.MergeTokensStep.index.combine_tokens_jmdict', return_value=merged):
            stage.process(Artifact([source]))
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
