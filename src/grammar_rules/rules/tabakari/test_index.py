import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import DA_DESU_EXPRESSION, TABAKARI_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.tabakari.index import match_tabakari
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TabakariTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '今食べたばかりだ。': 'ばかり',
            'さっき読んだばかりです。': 'ばかり',
            '帰ってきたばかりの人。': 'ばかり',
            '食べたばっかりだ。': 'ばっかり',
            '食べたばっかだ。': 'ばっか',
            '食べるばかりだ。': None,
            '彼ばかり食べる。': None,
            '静かだったばかりに失敗した。': None,
            '高かったばかりだ。': None,
            '嘘をついたばかりに困った。': None,
            '食べた ばかりだ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tabakari(aligned)],
                                 [] if expected is None else [expected], raw)

    def test_card_preserves_verb_and_evidence(self):
        source = sentence('私は今ご飯を食べたばかりだ。', [
            tok('私は今ご飯を', 'ご飯', ('名詞',)),
            tok('食べ', '食べる', ('動詞',)), tok('た', 'た', ('助動詞',)),
            tok('ばかり', 'ばかり', ('助詞', '副助詞')),
            tok('だ', 'だ', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['ばかり', 'だ'])
        words = {'食べる': object()}
        stage.add_cards(words)
        self.assertIn('食べる', words)
        self.assertEqual(words[TABAKARI_EXPRESSION].frequency, 2)
        self.assertEqual(words[TABAKARI_EXPRESSION].sentences[0].surface_form, 'ばかり')
        self.assertEqual(words[DA_DESU_EXPRESSION].sentences[0].surface_form, 'だ')


if __name__ == '__main__':
    unittest.main()
