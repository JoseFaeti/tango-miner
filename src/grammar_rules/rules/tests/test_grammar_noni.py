import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarShadow import aligned_tokens
from src.grammar_rules.rules.noni import RULE, match_noni
from src.grammar_rules.rules.tests.test_grammar_te_iru import sentence, tok


class NoniTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨が降っているのに出かける。': 'のに',
            '学生なのに知らない。': 'のに',
            '静かなのに驚いた。': 'のに',
            '読むのに時間がかかる。': 'のに',
            '昨日買ったのに傷がある。': 'のに',
            '赤いのにする。': 'のに',
            '行くのに。': 'のに',
            '彼の庭にいる。': None,
            'のに。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_noni(aligned)],
                                 [expected] if expected else [])

    def test_card_and_reservation(self):
        source = sentence('読むのに時間がかかる。', [
            tok('読む', '読む', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('の', 'の', ('助詞', '準体助詞')),
            tok('に', 'に', ('助詞', '格助詞')),
            tok('時間がかかる。', '時間', ('名詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['の', 'に'])
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'のに')
        self.assertIn('読む', words)
