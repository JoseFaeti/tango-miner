import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.nashidesumu.index import (
    NASHIDESUMU_EXPRESSION,
    RULE,
    match_nashidesumu,
)
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, RULES


class NashidesumuTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(
                config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '予約なしで済む。': 'なしで済む', '薬無しで済んだ。': '無しで済んだ',
            '連絡なしで済みました。': 'なしで済みました',
            '彼なしで済めばいい。': 'なしで済めば',
            'なしで済む。': None, '梨で済む。': None,
            '予約なしで住む。': None, '予約なしで帰る。': None,
            '予約なしで 済む。': None, '予約なしでは済まない。': None,
            '行かなくて済む。': None, '予約なしで済まない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nashidesumu(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_head_evidence(self):
        source = sentence('明日は予約なしで済むと思います。', [
            tok('明日は', '明日', ('名詞',)), tok('予約', '予約', ('名詞',)),
            tok('なし', 'なし', ('名詞',)),
            tok('で', 'で', ('助詞', '格助詞')), tok('済む', '済む', ('動詞', '一般')),
            tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep(rules=(RULE,))
        artifact = stage.process(Artifact([source]))
        reserved = ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in reserved.data[0].tokens
                          if t.get('grammar_reserved')], ['なし', 'で', '済む'])
        words = {'予約': object()}
        stage.add_cards(words)
        self.assertIn('予約', words)
        self.assertEqual(words[NASHIDESUMU_EXPRESSION].sentences[0].surface_form, 'なしで済む')
        self.assertIn(RULE, RULES)
        self.assertIn(NASHIDESUMU_EXPRESSION, GRAMMAR_EXPRESSIONS)


if __name__ == '__main__':
    unittest.main()
