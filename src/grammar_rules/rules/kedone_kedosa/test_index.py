import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.kedo_keredomo.index import RULE as KEDO_RULE
from src.grammar_rules.rules.kedone_kedosa.index import RULE, match_kedone_kedosa
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KedoneKedosaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'行ったけどね。': 'けどね', '高いけどさ。': 'けどさ',
                 '学生だけどね。': 'けどね', '行きますけどさ。': 'けどさ',
                 '行かないけどね。': 'けどね', 'けどね、行こう。': None,
                 '行くけど、ね。': None, '火傷が痛い。': None,
                 '行くけれどね。': None, '行くけど。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kedone_kedosa(aligned)],
                                 [expected] if expected else [], raw)

    def test_overlap_reservation_and_example(self):
        source = sentence('読むけどね今日は休みます。', [
            tok('読む', '読む', ('動詞', '一般', '*', '*', '五段', '終止形-一般')),
            tok('けど', 'けど', ('助詞', '接続助詞')),
            tok('ね', 'ね', ('助詞', '終助詞')),
            tok('今日は休みます。', '今日', ('名詞',))])
        stage = GrammarStep(rules=(KEDO_RULE, RULE))
        data = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['けど', 'ね'])
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'けどね')
        self.assertEqual(words[KEDO_RULE.expression].sentences[0].surface_form, 'けど')
        self.assertIn('読む', words)

    def test_particle_boundaries_and_pos(self):
        for text, pos in [('読むけど ね', ('助詞', '終助詞')),
                          ('読むけどね', ('名詞',))]:
            raw = [tok('読む', '読む', ('動詞', '一般', '*', '*', '五段', '終止形-一般')),
                   tok('けど', 'けど', ('助詞', '接続助詞')), tok('ね', 'ね', pos)]
            aligned, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_kedone_kedosa(aligned)), [])
