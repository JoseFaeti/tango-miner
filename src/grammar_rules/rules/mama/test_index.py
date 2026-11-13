import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.mama.index import RULE, match_mama
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MamaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'窓を開けたまま寝た。': ['まま'], '靴を履いたまま入る。': ['まま'],
                 '何も食べないまま出かけた。': ['まま'], '若いままでいたい。': ['まま'],
                 '静かなままだ。': ['まま'], '昔のままだ。': ['まま'],
                 'そのままにして。': [], 'ママが来た。': [],
                 'わがままな子だ。': [], 'ままごとをする。': [],
                 '窓を開けた まま寝た。': []}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_mama(tokens)], expected, raw)

    def test_reservation_and_identity(self):
        source = sentence('窓を開けたまま寝た。', [
            tok('窓を', '窓', ('名詞',)), tok('開け', '開ける', ('動詞',)),
            tok('た', 'た', ('助動詞',)), tok('まま', 'まま', ('名詞', '普通名詞')),
            tok('寝た。', '寝る', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')], ['まま'])
        words = {'窓': object()}
        stage.add_cards(words)
        self.assertIn('窓', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'まま')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
