import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.omotsute.index import RULE, match_omotsute
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class OmotsuteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'本日をもって終了します。': 'をもって',
                 '努力をもって成功した。': 'をもって',
                 '誠意を以て対応する。': 'を以て',
                 '本日をもちまして終了します。': 'をもちまして',
                 '彼は鞄を持って帰った。': 'を持って',
                 '本をもっている。': 'をもって',
                 '本日を もって終了する。': None,
                 '本日にもってくる。': None,
                 '本を持った。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_omotsute(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('本日をもって営業を終了します。', [
            tok('本日', '本日', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('もっ', 'もつ', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('営業を終了します。', '営業', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', 'もっ', 'て'])
        words = {'本日': object()}
        stage.add_cards(words)
        self.assertIn('本日', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'をもって')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
