import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.ndakedo_ndesuga.index import RULE, match_ndakedo_ndesuga
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NdakedoTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行くんだけど、どうする？': 'んだけど',
            '行きたいんですが。': 'んですが',
            '学生なんだけど。': 'んだけど',
            '静かなんですけれども。': 'んですけれども',
            '行くのですが、': 'のですが',
            '行ったのだけれど、': 'のだけれど',
            '本を読んだけど。': None,
            '行くけど。': None,
            '僕のですが。': None,
            '行くんだ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ndakedo_ndesuga(aligned)],
                                 [expected] if expected else [], raw)

    def test_reservation_identity_and_lexical_evidence(self):
        source = sentence('明日は学校に行くんだけど。', [
            tok('明日は学校に', '学校', ('名詞',)),
            tok('行く', '行く', ('動詞', '一般', '*', '*', '五段', '終止形-一般')),
            tok('ん', 'ん', ('助詞', '準体助詞')),
            tok('だ', 'だ', ('助動詞', '助動詞-ダ', '*', '*', '*', '終止形-一般')),
            tok('けど', 'けど', ('助詞', '接続助詞')),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['ん', 'だ', 'けど'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'んだけど')
        self.assertIn('行く', words)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)

    def test_separated_and_wrong_pos(self):
        for text, pos in [('行くんだ けど', ('助詞', '接続助詞')),
                          ('行くんだけど', ('名詞',))]:
            raw = [tok('行く', '行く', ('動詞',)), tok('ん', 'ん', ('助詞', '準体助詞')),
                   tok('だ', 'だ', ('助動詞',)), tok('けど', 'けど', pos)]
            aligned, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_ndakedo_ndesuga(aligned)), [])
