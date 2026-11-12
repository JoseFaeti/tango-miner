import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.baii_taraii.index import RULE, match_baii_taraii
from src.grammar_rules.rules.ba.index import RULE as BA
from src.grammar_rules.rules.tara.index import RULE as TARA
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class BaiiTaraiiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            'どうすればいい？': 'ばいい', '明日行ったらいい。': 'たらいい',
            '本を読んだら良い。': 'だら良い', '行かなければいい。': 'ばいい',
            '雨が降らなかったらいい。': 'たらいい', '天気がよければいい。': 'ばいい',
            '時間があればよい。': 'ばよい', '行けばよかった。': None,
            '行ったらよくない。': None, '行けば悪い。': None,
            '行けば いい。': None, 'たらいい。': None, '例えばいい人だ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_baii_taraii(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_overlapping_conditional_card(self):
        source = sentence('この本を読めばいいですよ。', [
            tok('この本を', '本', ('名詞',)),
            tok('読め', '読む', ('動詞', '一般', '*', '*', '五段', '仮定形-一般')),
            tok('ば', 'ば', ('助詞', '接続助詞')),
            tok('いい', '良い', ('形容詞', '一般', '*', '*', '形容詞', '終止形-一般')),
            tok('ですよ。', 'です', ('助動詞',))])
        stage = GrammarStep(rules=(RULE, BA, TARA))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ば', 'いい'])
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertIn('読む', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ばいい')


if __name__ == '__main__':
    unittest.main()
