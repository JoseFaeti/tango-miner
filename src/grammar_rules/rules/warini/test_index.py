import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.warini.index import RULE, match_warini
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class WariniTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            'よく食べるわりに太らない。': 'わりに',
            '勉強しなかった割に成績がよかった。': '割に',
            '安いわりにおいしい。': 'わりに',
            '静かなわりに便利だ。': 'わりに',
            '子供のわりに背が高い。': 'わりに',
            '年齢の割に若い。': '割に',
            '値段の割りにおいしい。': '割りに',
            '割に合わない。': None,
            '仕事の割に不満がある。': '割に',
            '三割に増えた。': None,
            '食べる わりに太らない。': None,
            '割りにする。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_warini(aligned)],
                                 [expected] if expected else [], raw)

    def test_card_reservation_and_identity(self):
        source = sentence('子供のわりに背が高い。', [
            tok('子供', '子供', ('名詞', '普通名詞')),
            tok('の', 'の', ('助詞', '格助詞')),
            tok('わり', 'わり', ('名詞', '普通名詞')),
            tok('に', 'に', ('助詞', '格助詞')),
            tok('背が高い。', '背', ('名詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['わり', 'に'])
        words = {'子供': object()}
        stage.add_cards(words)
        self.assertIn('子供', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'わりに')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
