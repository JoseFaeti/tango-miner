import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.karaaru_karano.index import RULE, match_karaaru_karano
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KaraaruKaranoTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '十人からの客が来た。': ['からの'], '百万円からある車。': ['からある'],
            '三十キロからある荷物。': ['からある'], '十年からの付き合い。': ['からの'],
            '三時からの会議。': ['からの'], '駅からの道。': [],
            '百人からいる。': [], '百万円 からある車。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_karaaru_karano(tokens)], expected)

    def test_identity_reservation_and_quantity_preservation(self):
        source = sentence('百人からの客が会場に集まりました。', [
            tok('百', '百', ('名詞', '数詞')), tok('人', '人', ('接尾辞', '名詞的')),
            tok('から', 'から', ('助詞', '格助詞')), tok('の', 'の', ('助詞', '格助詞')),
            tok('客が会場に集まりました。', '客', ('名詞',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['から', 'の'])
        words = {'百': object(), '人': object()}
        stage.add_cards(words)
        self.assertIn('百', words)
        self.assertIn('人', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'からの')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
