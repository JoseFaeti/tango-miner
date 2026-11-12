import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.toiukotoha.index import RULE, match_toiukotoha
from src.grammar_rules.rules.toiu.index import RULE as SHORT
from src.grammar_rules.rules.tte.index import RULE as TTE


class ToiukotohaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'彼が来るということは、会えるね。': 'ということは',
                 '雨だと言う事は、中止だ。': 'と言う事は',
                 '彼が来るっていうことは、会えるね。': 'っていうことは',
                 '彼が来るということはない。': None,
                 '彼が来るということだ。': None, '猫ということは何だ。': None,
                 '彼が来ると いうことは、嬉しい。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toiukotoha(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('彼が行くっていうことは嬉しい。', [
            tok('彼', '彼', ('名詞',)), tok('が', 'が', ('助詞',)),
            tok('行く', '行く', ('動詞', '一般', '*', '*', '五段-カ行', '終止形-一般')),
            tok('って', 'って', ('助詞',)), tok('いう', '言う', ('動詞',)),
            tok('こと', 'こと', ('名詞', '普通名詞')), tok('は', 'は', ('助詞',)),
            tok('嬉しい', '嬉しい', ('形容詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, SHORT, TTE))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['って', 'いう', 'こと', 'は'])
        self.assertEqual(stage.occurrences[SHORT.id].count, 0)
        self.assertEqual(stage.occurrences[TTE.id].count, 0)
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'っていうことは')
        self.assertIn(RULE, RULES)
