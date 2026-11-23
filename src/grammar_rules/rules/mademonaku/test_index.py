import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.mademonaku.index import RULE, match_mademonaku
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MademonakuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'言うまでもなく明らかだ。': 'までもなく',
                 '調べるまでもなく分かる。': 'までもなく',
                 '駅までもなく歩く。': None, '行ったまでもなく分かる。': None,
                 '行かないまでもなく分かる。': None, '行くまでもない。': None,
                 '行く までもなく分かる。': None, '行くまで もなく分かる。': None,
                 '行くまでも なく分かる。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_mademonaku(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('言うまでもなく明らかだ。', [
            tok('言う', '言う', ('動詞', '一般', '*', '*', '五段-ワア行', '終止形-一般')),
            tok('まで', 'まで', ('助詞', '副助詞')), tok('も', 'も', ('助詞', '係助詞')),
            tok('なく', 'ない', ('形容詞',)), tok('明らか', '明らか', ('形状詞',)),
            tok('だ', 'だ', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['まで', 'も', 'なく'])
        words = {'言う': object()}
        stage.add_cards(words)
        self.assertIn('言う', words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        from src.grammar_rules.registry import RULES
        from src.GrammarConstants import GRAMMAR_EXPRESSIONS
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertIn(RULE, RULES)
