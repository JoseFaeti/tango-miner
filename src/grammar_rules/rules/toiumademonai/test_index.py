import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.mademonai.index import RULE as GENERAL_RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.toiumademonai.index import RULE, match_toiumademonai


class ToiumademonaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            'それは当然と言うまでもない。': 'と言うまでもない',
            '当然というまでもなかった。': 'というまでもなかった',
            '当然と言うまでもありません。': 'と言うまでもありません',
            '当然というまでもありませんでした。': 'というまでもありませんでした',
            '言うまでもない。': None, '行くまでもない。': None,
            'と書くまでもない。': None, 'と言ったまでもない。': None,
            'と言うまでもなく分かる。': None, 'と 言うまでもない。': None,
            'と言う までもない。': None, 'と言うまでもある。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toiumademonai(aligned)],
                                 [] if expected is None else [expected])

    def test_overlap_reservation_and_identity(self):
        source = sentence('それは当然と言うまでもないことだ。', [
            tok('それ', 'それ', ('代名詞',)), tok('は', 'は', ('助詞',)),
            tok('当然', '当然', ('名詞',)), tok('と', 'と', ('助詞',)),
            tok('言う', '言う', ('動詞', '一般', '*', '*', '五段-ワア行', '連体形-一般')),
            tok('まで', 'まで', ('助詞',)), tok('も', 'も', ('助詞',)),
            tok('ない', 'ない', ('形容詞',)), tok('こと', 'こと', ('名詞',)),
            tok('だ', 'だ', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, GENERAL_RULE))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['と', '言う', 'まで', 'も', 'ない'])
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[GENERAL_RULE.expression].frequency, 1)
        self.assertEqual(stage.occurrences[RULE.id].examples[0].surface_form, 'と言うまでもない')
        from src.grammar_rules.registry import RULES
        from src.GrammarConstants import GRAMMAR_EXPRESSIONS
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertIn(RULE, RULES)
