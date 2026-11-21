import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.madenokotoda.index import RULE, match_madenokotoda
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MadenokotodaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '失敗したらもう一度やるまでのことだ。': 'までのことだ',
            '嫌なら辞めるまでのことです。': 'までのことです',
            '断られたら帰るまでの事だ。': 'までの事だ',
            '昨日までのことだ。': None, '駅までのことだ。': None,
            '帰るまでの道だ。': None, '帰るまでのことだった。': None,
            '帰る までのことだ。': None, '帰るまで のことだ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_madenokotoda(aligned)],
                                 [] if expected is None else [expected], raw)

    def test_card_and_reservation_preserve_verb(self):
        source = sentence('断られたら帰るまでのことだ。', [
            tok('断られたら', '断る', ('動詞',)),
            tok('帰る', '帰る', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('まで', 'まで', ('助詞', '副助詞')), tok('の', 'の', ('助詞', '格助詞')),
            tok('こと', 'こと', ('名詞', '普通名詞')), tok('だ', 'だ', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        artifact = stage.process(Artifact([source]))
        words = {'帰る': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'までのことだ')
        self.assertIn('帰る', words)
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens
                          if t.get('grammar_reserved')], ['まで', 'の', 'こと', 'だ'])
