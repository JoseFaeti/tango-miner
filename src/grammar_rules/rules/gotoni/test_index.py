import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GOTONI_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.gotoni.index import match_gotoni
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class GotoniTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '一時間ごとに休む。': 'ごとに', '行くごとに変わる。': 'ごとに',
            '仕事に行く。': None, '事にする。': None, '見事に勝つ。': None,
            'ごとに違う。': None, '一時間ごとは休む。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_gotoni(aligned)],
                                 [] if expected is None else [expected])

    def test_card_preserves_attached_word_and_evidence(self):
        source = sentence('一時間ごとにしっかり休みます。', [
            tok('一', '一', ('名詞', '数詞')), tok('時間', '時間', ('名詞',)),
            tok('ごと', 'ごと', ('接尾辞', '名詞的')),
            tok('に', 'に', ('助詞', '格助詞')), tok('しっかり休みます。', '休む', ('動詞',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['ごと', 'に'])
        words = {'時間': object()}
        stage.add_cards(words)
        self.assertIn('時間', words)
        self.assertEqual(words[GOTONI_EXPRESSION].frequency, 2)
        self.assertEqual(words[GOTONI_EXPRESSION].sentences[0].surface_form, 'ごとに')
