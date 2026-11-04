import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.kedo_keredomo.index import RULE, match_kedo
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KedoTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨が降るけど行く。': 'けど',
            '高いけれど買う。': 'けれど',
            '学生だけれども働く。': 'けれども',
            '静かですけど。': 'けど',
            '行ったけどね。': 'けど',
            '行かないけれども。': 'けれども',
            'けど、行こう。': None,
            '火傷が痛い。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kedo(aligned)],
                                 [expected] if expected else [], raw)

    def test_reservation_and_lexical_evidence(self):
        source = sentence('読むけど今日は休みます。', [
            tok('読む', '読む', ('動詞', '一般', '*', '*', '五段', '終止形-一般')),
            tok('けど', 'けど', ('助詞', '接続助詞')),
            tok('今日は休みます。', '今日', ('名詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')], ['けど'])
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'けど')
        self.assertIn('読む', words)

    def test_mismatched_pos_and_separated_tokens(self):
        for text, marker in [('読む けど', tok('けど', 'けど', ('助詞', '接続助詞'))),
                             ('読むけど', tok('けど', 'けど', ('名詞',)))]:
            raw = [tok('読む', '読む', ('動詞', '一般', '*', '*', '五段', '終止形-一般')), marker]
            aligned, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_kedo(aligned)), [])
