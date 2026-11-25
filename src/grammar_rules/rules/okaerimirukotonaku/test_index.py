import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.okaerimirukotonaku.index import (
    RULE,
    match_okaerimirukotonaku,
)
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class OkaerimirukotonakuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '年齢を顧みることなく参加できる。': 'を顧みることなく',
            '国籍をかえりみることなく募集する。': 'をかえりみることなく',
            '危険を顧みることなく進む。': 'を顧みることなく',
            '彼を顧みることなく去った。': 'を顧みることなく',
            '過去を省みることなく進む。': None,
            '危険を顧みる事なく進む。': None,
            '危険を顧みることはなく進む。': None,
            '危険を顧みること な生活。': None,
            '危険を顧みることなくても進む。': 'を顧みることなく',
            '年齢を顧みることがない。': None,
            '年齢を顧みず参加できる。': None,
            '顧みることなく終わった。': None,
            '年齢を 顧みることなく参加できる。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_okaerimirukotonaku(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('年齢を顧みることなく参加できる。', [
            tok('年齢', '年齢', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('顧みる', '顧みる', ('動詞',)), tok('こと', 'こと', ('名詞',)),
            tok('なく', 'ない', ('形容詞',)), tok('参加できる。', '参加', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '顧みる', 'こと', 'なく'])
        words = {'年齢': object()}
        stage.add_cards(words)
        self.assertIn('年齢', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を顧みることなく')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
