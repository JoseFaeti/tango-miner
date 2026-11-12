import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nante.index import RULE, match_nante
from src.grammar_rules.rules.nado_nanka_nante.index import RULE as COMBINED
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NanteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行くなんて信じられない。': ['なんて'], '猫なんて嫌いだ。': ['なんて'],
            'こんなことがあるなんて。': ['なんて'], '本なんて読まない。': ['なんて'],
            'なんて美しい。': [], '何て言った？': [], 'なんとかする。': [],
            '猫 なんて嫌いだ。': [], '本などを読む。': [], '私なんかには無理だ。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nante(tokens)], expected)

    def test_reservation_identity_and_overlap(self):
        source = sentence('私は猫なんて嫌いだ。', [tok('私は', '私', ('代名詞',)),
            tok('猫', '猫', ('名詞',)),
            tok('なんて', 'なんて', ('助詞', '副助詞')),
            tok('嫌い', '嫌い', ('形状詞',)), tok('だ', 'だ', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, COMBINED))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')], ['なんて'])
        self.assertEqual(stage.occurrences[COMBINED.id].count, 0)
        words = {'猫': object()}
        stage.add_cards(words)
        self.assertIn('猫', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'なんて')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
