import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.dokorodehanaku.index import RULE, match_dokorodehanaku
from src.grammar_rules.rules.dokorodehanai.index import RULE as NEGATIVE_RULE
from src.grammar_rules.rules.dehanaku.index import RULE as COPULA_RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class DokorodehanakuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '楽しいどころではなく、苦痛だった。': 'どころではなく',
            '寝るどころじゃなく、働いた。': 'どころじゃなく',
            'それどころでなく困った。': 'どころでなく',
            '寝るどころではない。': None,
            '寝るどころではなかった。': None,
            '寝るどころではなくて困った。': None,
            '雨どころか雪だ。': None,
            'どころではなく、困った。': None,
            '雨、どころではなく雪だ。': None,
            '寝るどころ ではなく働いた。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_dokorodehanaku(tokens)],
                                 [] if expected is None else [expected])

    def test_overlap_reservation_and_identity(self):
        source = sentence('仕事どころではなく、休んだ。', [
            tok('仕事', '仕事', ('名詞',)), tok('どころ', 'どころ', ('助詞', '副助詞')),
            tok('で', 'だ', ('助動詞',)), tok('は', 'は', ('助詞', '係助詞')),
            tok('なく', 'ない', ('形容詞',)), tok('、', '、', ('補助記号',)),
            tok('休ん', '休む', ('動詞',)), tok('だ', 'た', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, NEGATIVE_RULE, COPULA_RULE))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['どころ', 'で', 'は', 'なく'])
        words = {'仕事': object()}
        stage.add_cards(words)
        self.assertIn('仕事', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'どころではなく')
        self.assertIn(NEGATIVE_RULE.expression, words)
        self.assertIn(COPULA_RULE.expression, words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
