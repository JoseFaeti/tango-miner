import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_kara.index import RULE, match_te_kara
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TeKaraTests(unittest.TestCase):
    def test_source_reservation_and_non_temporal_control(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = [
            ('劫火と戦ってからというもの自分の力に自信がついた気がする。', ['て', 'から']),
            ('今度岩を斬るときは、甲羅かどうか確認してからにするのじゃぞ。', ['て', 'から']),
            ('のうがきは、速の強聖のオラに勝ってからにしな！', ['て', 'から']),
            ('よし、とにかく今日は休んで、明日からに備えようぜ。', []),
        ]
        for text, added in cases:
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                reserved = []
                for rules in (tuple(r for r in RULES if r.id != RULE.id), RULES):
                    stage = GrammarStep(rules=rules)
                    data = stage.process(Artifact([sentence(text, raw)]))
                    tokens = ExcludeGrammarTokensStep(stage).process(data).data[0].tokens
                    reserved.append([i for i, t in enumerate(tokens) if t.get('grammar_reserved')])
                self.assertEqual([raw[i]['surface'] for i in reserved[1] if i not in reserved[0]], added)

    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '手を洗ってから寝る。': 'てから', '本を読んでから行く。': 'でから',
            '日本に来てから三年経った。': 'てから', '準備してから出る。': 'てから',
            '駅から歩く。': None, '暑いから帰る。': None,
            '手から落ちた。': None, '食べたから帰る。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_te_kara(aligned)],
                                 [] if expected is None else [expected])

    def test_boundaries_and_reservation(self):
        source = sentence('手を洗ってから寝る。', [
            tok('手を', '手', ('名詞',)),
            tok('洗っ', '洗う', ('動詞', '一般', '*', '*', '五段', '連用形-促音便')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('から', 'から', ('助詞', '格助詞')),
            tok('寝る。', '寝る', ('動詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = stage.process(Artifact([source]))
        reserved = ExcludeGrammarTokensStep(stage).process(data).data[0].tokens
        self.assertEqual([t['surface'] for t in reserved if t.get('grammar_reserved')], ['て', 'から'])
        self.assertFalse(reserved[1].get('grammar_reserved'))
        words = {'洗う': object()}
        stage.add_cards(words)
        self.assertIn('洗う', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'てから')

        aligned, error = aligned_tokens(source.text, source.tokens)
        self.assertIsNone(error)
        aligned[3]['span'] = [aligned[3]['span'][0] + 1, aligned[3]['span'][1]]
        self.assertEqual(list(match_te_kara(aligned)), [])


if __name__ == '__main__':
    unittest.main()
