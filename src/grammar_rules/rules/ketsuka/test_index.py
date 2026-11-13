import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.ketsuka.index import KETSUKA_EXPRESSION, match_ketsuka
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KetsukaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '調べた結果、分かった。': '結果',
            '話し合った結果、決めた。': '結果',
            '調査の結果、分かった。': '結果',
            '検査の結果が出た。': '結果',
            '結果が出た。': None,
            '良い結果になった。': None,
            '調べる結果、分かった。': None,
            '調べた 結果、分かった。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ketsuka(tokens)],
                                 [] if expected is None else [expected])

    def test_card_preserves_process_and_reserves_result(self):
        source = sentence('調査の結果、分かった。', [
            tok('調査', '調査', ('名詞',)), tok('の', 'の', ('助詞', '格助詞')),
            tok('結果', '結果', ('名詞',)), tok('、', '、', ('補助記号',)),
            tok('分かった。', '分かる', ('動詞',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['結果'])
        words = {'調査': object()}
        stage.add_cards(words)
        self.assertIn('調査', words)
        self.assertEqual(words[KETSUKA_EXPRESSION].frequency, 2)
        self.assertEqual(words[KETSUKA_EXPRESSION].sentences[0].surface_form, '結果')
