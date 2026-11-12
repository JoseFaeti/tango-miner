import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import KADOUKA_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.kadouka.index import match_kadouka
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KadoukaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行くかどうか分からない。': 'かどうか',
            '行かないかどうか確認する。': 'かどうか',
            '高いかどうか聞く。': 'かどうか',
            '静かかどうか調べる。': 'かどうか',
            '学生かどうか知らない。': 'かどうか',
            '雨だったかどうか覚えていない。': 'かどうか',
            '行きますかどうか確認する。': 'かどうか',
            'どうか助けてください。': None,
            '彼はどうかと思う。': None,
            '行くか、どうか教えて。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kadouka(tokens)],
                                 [] if expected is None else [expected], raw)

    def test_card_reserves_only_expression(self):
        source = sentence('行くかどうか分からない。', [
            tok('行く', '行く', ('動詞',)), tok('か', 'か', ('助詞',)),
            tok('どう', 'どう', ('副詞',)), tok('か', 'か', ('助詞',)),
            tok('分からない。', '分かる', ('動詞',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['か', 'どう', 'か'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[KADOUKA_EXPRESSION].frequency, 2)
        self.assertEqual(words[KADOUKA_EXPRESSION].sentences[0].surface_form, 'かどうか')

    def test_nonadjacent_and_lexical_tokens(self):
        for text, raw in [
            ('行くか どうか', [tok('行く', '行く', ('動詞',)), tok('か', 'か', ('助詞',)), tok('どう', 'どう', ('副詞',)), tok('か', 'か', ('助詞',))]),
            ('行くかどうか', [tok('行く', '行く', ('動詞',)), tok('か', 'か', ('助詞',)), tok('どう', 'どう', ('名詞',)), tok('か', 'か', ('助詞',))]),
        ]:
            tokens, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_kadouka(tokens)), [])
