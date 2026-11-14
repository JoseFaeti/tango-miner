import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.he.index import RULE, match_he
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class HeTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'学校へ行く。': 1, '未来への希望。': 1, '友達へ手紙を書く。': 1,
                 '遠くへ。': 1, '「東京」へ行く。': 1, '家へ帰り学校へ行く。': 2,
                 '学校 へ行く。': 0, 'へ行く。': 0, 'へえ、そうか。': 0,
                 '部屋を見る。': 0}
        for text, count in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_he(tokens)], ['へ'] * count)

    def test_reservation_and_identity(self):
        source = sentence('学校へ毎日歩いて行きます。', [
            tok('学校', '学校', ('名詞',)), tok('へ', 'へ', ('助詞', '格助詞')),
            tok('毎日', '毎日', ('名詞',)), tok('歩いて', '歩く', ('動詞',)),
            tok('行きます', '行く', ('動詞',)),
            tok('。', '。', ('補助記号', '句点'))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')], ['へ'])
        words = {'学校': object(), '行く': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'へ')
        self.assertTrue(all(word in words for word in ('学校', '行く')))
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)

    def test_non_particle_and_punctuation_controls(self):
        for previous, particle in [
            (tok('学校', '学校', ('名詞',)), tok('へ', 'へ', ('感動詞',))),
            (tok('、', '、', ('補助記号', '読点')), tok('へ', 'へ', ('助詞', '格助詞'))),
            (tok('学校', '学校', ('名詞',)), tok('へ', '違う', ('助詞', '格助詞'))),
        ]:
            text = previous['surface'] + particle['surface']
            tokens, error = aligned_tokens(text, [previous, particle])
            self.assertIsNone(error)
            self.assertEqual(list(match_he(tokens)), [])
