import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.mademonai.index import RULE, match_mademonai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MademonaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'言うまでもない。': 'までもない', '心配するまでもなかった。': 'までもなかった',
                 '調べるまでもありません。': 'までもありません',
                 '聞くまでもありませんでした。': 'までもありませんでした',
                 '駅までもない。': None, '行ったまでもない。': None,
                 '行かないまでもない。': None, '行くまでもある。': None,
                 '行くまでもなく分かる。': None, '行く までもない。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_mademonai(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('それは言うまでもないことだ。', [
            tok('それ', 'それ', ('代名詞',)), tok('は', 'は', ('助詞',)),
            tok('言う', '言う', ('動詞', '一般', '*', '*', '五段-ワア行', '連体形-一般')),
            tok('まで', 'まで', ('助詞', '副助詞')), tok('も', 'も', ('助詞', '係助詞')),
            tok('ない', 'ない', ('形容詞',)), tok('こと', 'こと', ('名詞',)),
            tok('だ', 'だ', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['まで', 'も', 'ない'])
        words = {'言う': object()}
        stage.add_cards(words)
        self.assertIn('言う', words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        from src.GrammarConstants import GRAMMAR_EXPRESSIONS
        from src.grammar_rules.registry import RULES
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertIn(RULE, RULES)
