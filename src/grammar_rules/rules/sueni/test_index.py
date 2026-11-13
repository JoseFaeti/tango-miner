import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.sueni.index import SUENI_EXPRESSION, match_sueni
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class SueniTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '考えた末に決めた。': '末に', '悩んだすえに決めた。': 'すえに',
            '話し合いの末に決めた。': '末に', '週末に会う。': None,
            '末にある。': None, '考える末に決めた。': None,
            '苦労の末の成功。': None, '考えた 末に決めた。': None,
            '長い道の末にある。': '末に',
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_sueni(tokens)],
                                 [] if expected is None else [expected])

    def test_card_preserves_process_and_reserves_expression(self):
        source = sentence('話し合いの末に決めた。', [
            tok('話し合い', '話し合い', ('名詞',)), tok('の', 'の', ('助詞', '格助詞')),
            tok('末', '末', ('名詞',)), tok('に', 'に', ('助詞', '格助詞')),
            tok('決めた。', '決める', ('動詞',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['末', 'に'])
        words = {'話し合い': object()}
        stage.add_cards(words)
        self.assertIn('話し合い', words)
        self.assertEqual(words[SUENI_EXPRESSION].frequency, 2)
        self.assertEqual(words[SUENI_EXPRESSION].sentences[0].surface_form, '末に')
