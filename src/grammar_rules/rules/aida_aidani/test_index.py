import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.aida_aidani.index import AIDA_AIDANI_EXPRESSION, RULE, match_aida_aidani
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class AidaAidaniTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '寝ている間に電話が鳴った。': '間に', '休みの間、勉強した。': '間',
            '忙しい間に帰る。': '間に', '元気な間に旅行する。': '間に',
            '食べない間に帰る。': '間に', 'しばらくのあいだ待った。': 'あいだ',
            '一週間の間に終える。': '間に', '間に合う。': None,
            '間に人がいる。': None, 'その間に帰る。': None,
            '二人の間に立つ。': '間に', '子供の間に人気だ。': '間に',
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_aida_aidani(aligned)],
                                 [] if expected is None else [expected])

    def test_card_reserves_phrase_and_keeps_attached_word(self):
        source = sentence('私は休みの間に家へ帰ります。', [
            tok('私は', '私', ('名詞',)),
            tok('休み', '休み', ('名詞',)), tok('の', 'の', ('助詞', '格助詞')),
            tok('間', '間', ('名詞', '普通名詞', '副詞可能')),
            tok('に', 'に', ('助詞', '格助詞')), tok('家へ帰ります。', '帰る', ('動詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['間', 'に'])
        words = {'休み': object()}
        stage.add_cards(words)
        self.assertIn('休み', words)
        self.assertEqual(words[AIDA_AIDANI_EXPRESSION].frequency, 2)
        self.assertEqual(words[AIDA_AIDANI_EXPRESSION].sentences[0].surface_form, '間に')

    def test_existing_ongoing_rule_also_collects_overlap(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        text = '私が寝ている間に電話が鳴りました。'
        raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
               for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
        stage = GrammarStep()
        stage.process(Artifact([sentence(text, raw)]))
        self.assertEqual(stage.occurrences['aida_aidani'].count, 1)
        self.assertEqual(stage.occurrences['teiru_aida_ni'].count, 1)


if __name__ == '__main__':
    unittest.main()
