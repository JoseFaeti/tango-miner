import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nikagiru.index import RULE, match_nikagiru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NikagiruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '夏はビールに限る。': 'に限る',
            '疲れた日は寝るに限る。': 'に限る',
            'そんなことは言わないに限る。': 'に限る',
            '参加者は会員に限ります。': 'に限ります',
            '温泉はここにかぎる。': 'にかぎる',
            '今日に限って雨だ。': None,
            '子供に限らず大人も楽しめる。': None,
            '時間を限る。': None,
            '会員に 限る。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nikagiru(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('暑い日は冷たいビールに限ると思います。', [
            tok('暑い日は冷たい', '暑い', ('形容詞',)),
            tok('ビール', 'ビール', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('限る', '限る', ('動詞',)), tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '限る'])
        words = {'ビール': object()}
        stage.add_cards(words)
        self.assertIn('ビール', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に限る')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
