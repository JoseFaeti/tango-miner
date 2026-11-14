import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.mononara.index import MONONARA_EXPRESSION, RULE, match_mononara
from src.grammar_rules.rules.nara.index import RULE as NARA_RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MononaraTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行けるものなら行きたい。': True,
            '戻れるものなら戻りたい。': True,
            'できるものならやってみろ。': True,
            # Literal nominal readings share the same tokenization.
            '買えるものなら何でもいい。': True,
            '買うものならこれだ。': True,
            '行こうものなら大変だ。': False,
            '行ける物なら欲しい。': False,
            '行けるものなら。': False,
            '私のものなら使っていい。': False,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_mononara(aligned)],
                                 ['ものなら'] if expected else [])

    def test_card_preserves_components_and_boundaries(self):
        verb = tok('行ける', '行ける', ('動詞', '*', '*', '*', '*', '連体形-一般'))
        mono = tok('もの', 'もの', ('名詞',))
        nara = tok('なら', 'だ', ('助動詞', '*', '*', '*', '*', '仮定形-一般'))
        tail = tok('行きたい', '行く', ('動詞',))
        source = sentence('行けるものなら行きたい', [verb, mono, nara, tail])
        stage = GrammarStep(rules=(RULE,))
        artifact = stage.process(Artifact([source, source]))
        reserved = ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertFalse(any(t.get('grammar_reserved') for t in reserved.data[0].tokens))
        words = {'行ける': object(), 'もの': object()}
        stage.add_cards(words)
        self.assertIn('行ける', words)
        self.assertIn('もの', words)
        self.assertEqual(words[MONONARA_EXPRESSION].frequency, 2)
        self.assertEqual(words[MONONARA_EXPRESSION].sentences[0].surface_form, 'ものなら')
        self.assertEqual(len(words[MONONARA_EXPRESSION].sentences), 1)
        for text, raw in [('行ける ものなら行きたい', [verb, mono, nara, tail]),
                          ('行けるもの なら行きたい', [verb, mono, nara, tail])]:
            aligned, error = aligned_tokens(text, raw)
            if error is None:
                self.assertEqual(list(match_mononara(aligned)), [])

    def test_overlapping_nara_remains_available(self):
        source = sentence('行けるものなら行きたい', [
            tok('行ける', '行ける', ('動詞', '*', '*', '*', '*', '連体形-一般')),
            tok('もの', 'もの', ('名詞',)),
            tok('なら', 'だ', ('助動詞', '*', '*', '*', '*', '仮定形-一般')),
            tok('行きたい', '行く', ('動詞',))])
        stage = GrammarStep(rules=(RULE, NARA_RULE))
        stage.process(Artifact([source]))
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        self.assertEqual(stage.occurrences[NARA_RULE.id].count, 1)
