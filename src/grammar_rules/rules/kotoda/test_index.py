import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import KOTODA_EXPRESSION, TOIUKOTODA_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.kotoda.index import match_kotoda
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KotodaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '毎日練習することだ。': 'ことだ',
            '早く寝ることです。': 'ことです',
            '心配しないことだ。': 'ことだ',
            'なんと美しいことだ。': 'ことだ',
            '静かなことだ。': 'ことだ',
            '彼が来るということだ。': None,
            '彼が来るっていうことだ。': None,
            '練習することになる。': None,
            '彼のことだ。': None,
            '練習することだった。': None,
            '練習する ことだ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kotoda(aligned)],
                                 [] if expected is None else [expected], raw)

    def test_card_evidence_and_reservation(self):
        source = sentence('毎日練習することだ。', [
            tok('毎日', '毎日', ('名詞',)), tok('練習', '練習', ('名詞',)),
            tok('する', 'する', ('動詞', '非自立可能', '*', '*', 'サ行変格', '連体形-一般')),
            tok('こと', 'こと', ('名詞', '普通名詞')),
            tok('だ', 'だ', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['こと', 'だ'])
        words = {'練習': object()}
        stage.add_cards(words)
        self.assertIn('練習', words)
        self.assertEqual(words[KOTODA_EXPRESSION].frequency, 2)
        self.assertEqual(words[KOTODA_EXPRESSION].sentences[0].surface_form, 'ことだ')
        self.assertNotIn(TOIUKOTODA_EXPRESSION, words)
