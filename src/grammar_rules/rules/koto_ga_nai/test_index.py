import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.koto_ga_nai.index import RULE, match_never
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NeverTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'彼は決して嘘をつくことがない。': 'ことがない',
                 '決して遅れることがなかった。': 'ことがなかった',
                 '決して遅れることがありません。': 'ことがありません',
                 '決して遅れることがありませんでした。': 'ことがありませんでした',
                 'やることがない。': None, '決して聞きたいことがない。': None,
                 '決して行ったことがない。': None,
                 '決して行かない。「読むことがない。」': None,
                 '読むことがない。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_never(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('決して遅れることがない。', [
            tok('決して', '決して', ('副詞',)),
            tok('遅れる', '遅れる', ('動詞', '一般', '*', '*', '下一段', '連体形-一般')),
            tok('ことがない', 'ことがない', ('形容詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')], ['ことがない'])
        words = {'遅れる': object()}
        stage.add_cards(words)
        self.assertIn('遅れる', words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        from src.GrammarConstants import GRAMMAR_EXPRESSIONS
        from src.grammar_rules.registry import RULES
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertIn(RULE, RULES)
