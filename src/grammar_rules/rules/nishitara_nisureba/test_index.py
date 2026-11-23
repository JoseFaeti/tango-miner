import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nishitara_nisureba.index import RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NishitaraNisurebaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'私にしたら意外だ。': 'にしたら', '彼にすれば当然だ。': 'にすれば',
                 '子供にしたら大変だ。': 'にしたら', '私に したら困る。': None,
                 '彼としたら困る。': None, '仕事をしたら帰る。': None,
                 '彼にしてみれば当然だ。': None, '私にする。': None,
                 'お茶にしたらどうですか。': None,
                 'ローストチキンにしたら食いごたえがあると思うぜ？': None,
                 'いいかげんに諦めて別ショットで記事にしたら？': None,
                 'りを前にすれば王女様だって元気をとりもどすかもしれないよ。': None,
                 '彼をリーダーにしたら困る。': None,
                 '彼を先生にすればいい。': None,
                 '彼を目の前にしたら驚いた。': None,
                 '先生にすれば当然だ。': 'にすれば',
                 '政府にしたら難しい問題だ。': 'にしたら',
                 '田中にすれば迷惑だ。': 'にすれば',
                 '私たちにしたら意外だ。': 'にしたら',
                 '彼を学生にすればいい。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in RULE.match(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('私にすればそれは当然のことです。', [
            tok('私', '私', ('代名詞',)), tok('に', 'に', ('助詞',)),
            tok('すれ', 'する', ('動詞',)), tok('ば', 'ば', ('助詞',)),
            tok('それは当然のことです。', '当然', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'すれ', 'ば'])
        words = {'私': object()}
        stage.add_cards(words)
        self.assertIn('私', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'にすれば')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
