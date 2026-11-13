import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.teiru_aida_ni.index import RULE, match_teiru_aida_ni
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TeiruAidaNiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '寝ている間に雨が降った。': 'ている間に',
            '遊んでいる間に帰った。': 'でいる間に',
            '寝てる間に雨が降った。': 'てる間に',
            '寝ているあいだに雨が降った。': 'ているあいだに',
            '寝ていた間に雨が降った。': None,
            '寝ている間は静かだ。': None,
            '会議の間に帰った。': None,
            '駅の間にある。': None,
            '寝ている 間に帰った。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_teiru_aida_ni(aligned)],
                                 [] if expected is None else [expected])

    def test_card_and_reservation(self):
        source = sentence('寝ている間に雨が降った。', [
            tok('寝', '寝る', ('動詞', '一般', '*', '*', '一段', '連用形-一般')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('いる', 'いる', ('動詞', '非自立可能', '*', '*', '一段', '連体形-一般')),
            tok('間', '間', ('名詞', '普通名詞')),
            tok('に', 'に', ('助詞', '格助詞')),
            tok('雨が降った。', '雨', ('名詞',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source, source])))
        self.assertFalse(result.data[0].tokens[0].get('grammar_reserved'))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['て', 'いる', '間', 'に'])
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ている間に')
