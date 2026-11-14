import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.zuni.index import RULE as SHORT_RULE
from src.grammar_rules.rules.zunisumu.index import RULE, match_zunisumu


class ZunisumuTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行かずに済む。': 'ずに済む', '行かずに済んだ。': 'ずに済んだ',
            '食べずに済みました。': 'ずに済みました', 'せずに済めばいい。': 'ずに済めば',
            '来ずに済まない。': 'ずに済まない', '行かずにすむ。': 'ずにすむ',
            '行かずに住む。': None, '行かずに帰る。': None,
            'ずに済む。': None, '行かずに 済む。': None,
            '行かずには済まない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_zunisumu(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_overlapping_cards(self):
        parts = [tok('明日は', '明日', ('名詞',)),
                 tok('行か', '行く', ('動詞', '一般', '*', '*', '*', '未然形-一般')),
                 tok('ず', 'ず', ('助動詞',)), tok('に', 'に', ('助詞',)),
                 tok('済ん', '済む', ('動詞',)), tok('だ', 'だ', ('助動詞',)),
                 tok('と思います。', 'と', ('助詞',))]
        stage = GrammarStep(rules=(SHORT_RULE, RULE))
        result = ExcludeGrammarTokensStep(stage).process(
            stage.process(Artifact([sentence('明日は行かずに済んだと思います。', parts)])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ず', 'に', '済ん', 'だ'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[SHORT_RULE.expression].sentences[0].surface_form, 'ずに')
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ずに済んだ')
