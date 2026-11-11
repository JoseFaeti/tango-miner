import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.masen_mashita.index import RULE, match_masen_mashita
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class PoliteEndingsTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'食べません。': ['ません'], '食べました。': ['ました'],
                 '食べませんでした。': ['ませんでした'], '読まれました。': ['ました'],
                 'ありませんでした。': ['ませんでした'], '行きませんか': ['ません'],
                 'すみません': ['ません'], '食べます。': [], '食べましたら': [],
                 '食べましたり': [], '猫でした。': [], '食べ ません。': []}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_masen_mashita(tokens)], expected)

    def test_reservation_and_identity(self):
        source = sentence('私は朝ご飯を食べません。', [
            tok('私は朝ご飯を', '私', ('名詞',)),
            tok('食べ', '食べる', ('動詞', '一般', '*', '*', '下一段', '連用形-一般')),
            tok('ませ', 'ます', ('助動詞',)), tok('ん', 'ぬ', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ませ', 'ん'])
        words = {'食べる': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ません')
        self.assertIn('食べる', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)

    def test_incomplete_and_wrong_tokens(self):
        stem = tok('食べ', '食べる', ('動詞', '一般', '*', '*', '下一段', '連用形-一般'))
        for text, raw in [('食べませ', [stem, tok('ませ', 'ます', ('助動詞',))]),
                          ('食べませんでし', [stem, tok('ませ', 'ます', ('助動詞',)),
                                        tok('ん', 'ぬ', ('助動詞',)), tok('でし', 'です', ('助動詞',))]),
                          ('食べました', [stem, tok('まし', 'ます', ('名詞',)), tok('た', 'た', ('助動詞',))])]:
            tokens, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_masen_mashita(tokens)), [])
