import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.mo.index import RULE, match_mo
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.te_mo.index import RULE as TE_MO
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class MoTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'猫も犬もいる。': 2, '学校にも行く。': 1, '誰も来ない。': 1,
                 '三時間も待った。': 1, '「猫」もいる。': 1,
                 '雨でも行く。': 1, '桃を食べる。': 0, 'もう行く。': 0,
                 '猫 もいる。': 0}
        for text, count in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_mo(tokens)], ['も'] * count)

    def test_reservation_identity_and_overlap(self):
        source = sentence('猫も雨が降っても行く。', [
            tok('猫', '猫', ('名詞',)), tok('も', 'も', ('助詞', '係助詞')),
            tok('雨', '雨', ('名詞',)), tok('が', 'が', ('助詞', '格助詞')),
            tok('降っ', '降る', ('動詞', '一般', '*', '*', '五段', '連用形-促音便')),
            tok('て', 'て', ('助詞', '接続助詞')), tok('も', 'も', ('助詞', '係助詞')),
            tok('行く', '行く', ('動詞', '一般')), tok('。', '。', ('補助記号', '句点'))])
        stage = GrammarStep(rules=(RULE, TE_MO))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['も', 'て', 'も'])
        self.assertEqual(stage.occurrences[TE_MO.id].count, 1)
        words = {'猫': object(), '雨': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'も')
        self.assertIn('猫', words)
        self.assertIn('雨', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)

    def test_rejects_non_particle_and_punctuation(self):
        for previous, particle in [
                (tok('猫', '猫', ('名詞',)), tok('も', '藻', ('名詞',))),
                (tok('、', '、', ('補助記号', '読点')), tok('も', 'も', ('助詞', '係助詞'))),
                (tok('猫', '猫', ('名詞',)), tok('も', 'も', ('助詞', '副助詞')))]:
            text = previous['surface'] + particle['surface']
            tokens, error = aligned_tokens(text, [previous, particle])
            self.assertIsNone(error)
            self.assertEqual(list(match_mo(tokens)), [])
