import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.okamawazu.index import RULE, match_okamawazu
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class OkamawazuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '人目をかまわず泣いた。': 'をかまわず',
            '雨を構わず出かけた。': 'を構わず',
            '人目をかまわずに泣いた。': 'をかまわず',
            '濡れるのをかまわず走った。': 'をかまわず',
            '服装をもかまわず出かけた。': None,
            '人目にかまわず泣いた。': None,
            '誰もかまわず出かけた。': None,
            '子供を構う。': None,
            '子供を構わずに放っておいた。': 'を構わず',
            '雨を構わない。': None,
            '天候にかかわらず開催する。': None,
            '人目を かまわず泣いた。': None,
            '人目 をかまわず泣いた。': None,
            'をかまわず': None,
            '。をかまわず': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_okamawazu(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('天候をかまわず開催する。', [
            tok('天候', '天候', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('かまわ', 'かまう', ('動詞',)), tok('ず', 'ず', ('助動詞',)),
            tok('開催する。', '開催', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', 'かまわ', 'ず'])
        words = {'天候': object()}
        stage.add_cards(words)
        self.assertIn('天候', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'をかまわず')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)

