import unittest
from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.toiunoha.index import RULE, match_toiunoha
from src.grammar_rules.rules.toiu.index import RULE as SHORT
from src.grammar_rules.rules.tte.index import RULE as TTE


class ToiunohaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'猫というのは動物です。': 'というのは',
                 '彼が来るというのは本当だ。': 'というのは',
                 '猫っていうのは動物です。': 'っていうのは',
                 '猫と言うのは変だ。': 'と言うのは',
                 '猫という動物が好きだ。': None,
                 '猫と いうのは変だ。': None, '猫というのが好きだ。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toiunoha(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('猫っていうのは動物だ。', [
            tok('猫', '猫', ('名詞',)), tok('って', 'って', ('助詞',)),
            tok('いう', '言う', ('動詞',)), tok('の', 'の', ('助詞',)),
            tok('は', 'は', ('助詞',)), tok('動物', '動物', ('名詞',)),
            tok('だ', 'だ', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, SHORT, TTE))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['って', 'いう', 'の', 'は'])
        self.assertEqual(stage.occurrences[SHORT.id].count, 0)
        self.assertEqual(stage.occurrences[TTE.id].count, 0)
        words = {'猫': object()}
        stage.add_cards(words)
        self.assertIn('猫', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'っていうのは')
        self.assertIn(RULE, RULES)
