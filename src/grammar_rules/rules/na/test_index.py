import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.kana_kashira.index import RULE as KANA_RULE
from src.grammar_rules.rules.na.index import RULE, match_na
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import NA_EXPRESSION


class NaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'行くな。': 'な', '行くなよ。': 'な', '綺麗だなあ。': 'なあ',
                 '雨だな。': 'な', 'そうだなと思う。': 'な', '行くかな。': None,
                 '静かな町': None, 'かなしい。': None, '行く な。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_na(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_kana_overlap(self):
        verb = tok('行く', '行く', ('動詞',))
        na = tok('な', 'な', ('助詞', '終助詞'))
        source = sentence('明日はあの場所へ行くな。', [tok('明日はあの場所へ', '場所', ('名詞',)), verb, na,
                                         tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, KANA_RULE))
        artifact = stage.process(Artifact([source, source]))
        reserved = ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in reserved.data[0].tokens
                          if t.get('grammar_reserved')], ['な'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertIn(RULE, RULES)
        self.assertEqual(words[NA_EXPRESSION].frequency, 2)
        self.assertEqual(words[NA_EXPRESSION].sentences[0].surface_form, 'な')
        kana = sentence('明日はあの場所へ行くかな。', [tok('明日はあの場所へ', '場所', ('名詞',)), verb,
                                          tok('か', 'か', ('助詞', '終助詞')), na,
                                          tok('。', '。', ('補助記号',))])
        stage.process(Artifact([kana]))
        self.assertEqual(stage.occurrences[RULE.id].count, 0)
        self.assertEqual(stage.occurrences[KANA_RULE.id].count, 1)
