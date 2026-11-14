import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.youmononara.index import RULE, YOUMONONARA_EXPRESSION, match_youmononara
from src.grammar_rules.rules.mononara.index import RULE as MONONARA
from src.grammar_rules.rules.nara.index import RULE as NARA
from src.grammar_rules.rules.mashou_you.index import RULE as VOLITIONAL
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class YoumononaraTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行こうものなら大変だ。': '行こうものなら',
            '食べようものなら怒られる。': '食べようものなら',
            'しようものなら困る。': 'しようものなら',
            '来ようものなら困る。': '来ようものなら',
            '行けるものなら行きたい。': None,
            '私のものなら使っていい。': None,
            '行こう物なら大変だ。': None,
            '行こうものなら。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_youmononara(aligned)],
                                 [] if expected is None else [expected])

    def test_components_overlaps_and_boundaries(self):
        verb = tok('行こう', '行く', ('動詞', '*', '*', '*', '*', '意志推量形'))
        mono = tok('もの', 'もの', ('名詞',))
        nara = tok('なら', 'だ', ('助動詞', '*', '*', '*', '*', '仮定形-一般'))
        tail = tok('大変なことになる。', '大変', ('形状詞',))
        source = sentence('行こうものなら大変なことになる。', [verb, mono, nara, tail])
        stage = GrammarStep(rules=(RULE, MONONARA, NARA, VOLITIONAL))
        stage.process(Artifact([source, source]))
        stage_only = GrammarStep(rules=(RULE,))
        artifact_only = stage_only.process(Artifact([source]))
        result = ExcludeGrammarTokensStep(stage_only).process(artifact_only)
        self.assertFalse(any(t.get('grammar_reserved') for t in result.data[0].tokens))
        words = {'行く': object(), 'もの': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertIn('もの', words)
        self.assertEqual(words[YOUMONONARA_EXPRESSION].frequency, 2)
        self.assertEqual(words[YOUMONONARA_EXPRESSION].sentences[0].surface_form, '行こうものなら')
        self.assertEqual(stage.occurrences[NARA.id].count, 2)
        self.assertEqual(stage.occurrences[VOLITIONAL.id].count, 2)
        self.assertEqual(stage.occurrences[MONONARA.id].count, 0)
        for text in ('行こう ものなら大変なことになる。', '行こうもの なら大変なことになる。'):
            aligned, error = aligned_tokens(text, [verb, mono, nara, tail])
            self.assertIsNone(error)
            self.assertEqual(list(match_youmononara(aligned)), [])
