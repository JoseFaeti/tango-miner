import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.madeda.index import RULE, match_madeda
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MadedaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '断られたら帰るまでだ。': 'までだ',
            '私は当然のことをしたまでです。': 'までです',
            '聞かれたから答えたまでだ。': 'までだ',
            '駅までだ。': None, '昨日までだ。': None,
            '帰るまでのことだ。': None, '帰るまでだった。': None,
            '帰る までだ。': None, '帰るまで だ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_madeda(aligned)],
                                 [] if expected is None else [expected], raw)

    def test_card_and_reservation_preserve_verb(self):
        source = sentence('断られたら帰るまでだ。', [
            tok('断られたら', '断る', ('動詞',)),
            tok('帰る', '帰る', ('動詞', '一般', '*', '*', '五段', '終止形-一般')),
            tok('まで', 'まで', ('助詞', '副助詞')), tok('だ', 'だ', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        artifact = stage.process(Artifact([source]))
        words = {'帰る': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'までだ')
        self.assertIn('帰る', words)
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens
                          if t.get('grammar_reserved')], ['まで', 'だ'])
