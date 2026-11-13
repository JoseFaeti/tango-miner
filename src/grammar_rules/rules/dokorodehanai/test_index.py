import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.dokorodehanai.index import RULE, match_dokorodehanai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class DokorodehanaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '寝るどころではない。': 'どころではない',
            '仕事どころじゃない。': 'どころじゃない',
            '旅行どころではなかった。': 'どころではなかった',
            '遊ぶどころじゃなかった。': 'どころじゃなかった',
            '勉強どころではありません。': 'どころではありません',
            '勉強どころではありませんでした。': 'どころではありませんでした',
            '寝るどころではなく、働く。': 'どころではなく',
            '雨どころか雪だ。': None,
            'どころではない。': None,
            '雨、どころではない。': None,
            '寝るどころ ではない。': None,
            '寝るどころではある。': None,
            '寝るどころではなかったらしい。': 'どころではなかった',
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_dokorodehanai(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_lexical_evidence(self):
        source = sentence('仕事どころじゃない。', [
            tok('仕事', '仕事', ('名詞',)), tok('どころ', 'どころ', ('助詞', '副助詞')),
            tok('じゃ', 'だ', ('助動詞',)), tok('ない', 'ない', ('形容詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['どころ', 'じゃ', 'ない'])
        words = {'仕事': object()}
        stage.add_cards(words)
        self.assertIn('仕事', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'どころじゃない')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
