import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.otoukotonaku.index import RULE, match_otoukotonaku
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class OtoukotonakuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '年齢を問うことなく参加できる。': 'を問うことなく',
            '国籍をとうことなく募集する。': 'をとうことなく',
            '年齢を問うことがない。': None,
            '年齢を問わず参加できる。': None,
            '問うことなく終わった。': None,
            '年齢を 問うことなく参加できる。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_otoukotonaku(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('年齢を問うことなく参加できる。', [
            tok('年齢', '年齢', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('問う', '問う', ('動詞',)), tok('こと', 'こと', ('名詞',)),
            tok('なく', 'ない', ('形容詞',)), tok('参加できる。', '参加', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '問う', 'こと', 'なく'])
        words = {'年齢': object()}
        stage.add_cards(words)
        self.assertIn('年齢', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を問うことなく')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
