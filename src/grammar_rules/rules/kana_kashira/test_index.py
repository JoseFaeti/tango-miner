import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import KANA_KASHIRA_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.kana_kashira.index import RULE, match_kana_kashira
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KanaKashiraTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行くかな。': 'かな', '雨かな？': 'かな', '静かかな。': 'かな',
            '行かないかな。': 'かな', '行くかなあ。': 'かなあ',
            '行くかしら。': 'かしら', '雨かしら。': 'かしら',
            'どうかなと思う。': 'かな', '魚かな。': 'かな',
            'かなしい。': None, 'かな文字': None, '行くか。': None,
            '行くか な。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kana_kashira(aligned)],
                                 [] if expected is None else [expected])

    def test_integration_and_boundaries(self):
        verb = tok('行く', '行く', ('動詞',))
        ka = tok('か', 'か', ('助詞', '終助詞'))
        na = tok('な', 'な', ('助詞', '終助詞'))
        source = sentence('明日はあの場所へ行くかな。', [
            tok('明日はあの場所へ', '場所', ('名詞',)), verb, ka, na,
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        artifact = stage.process(Artifact([source, source]))
        reserved = ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in reserved.data[0].tokens
                          if t.get('grammar_reserved')], ['か', 'な'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertIn(RULE, RULES)
        self.assertEqual(words[KANA_KASHIRA_EXPRESSION].frequency, 2)
        self.assertEqual(words[KANA_KASHIRA_EXPRESSION].sentences[0].surface_form, 'かな')
        for text in ('行く かな', '行くか な'):
            aligned, error = aligned_tokens(text, [verb, ka, na])
            self.assertIsNone(error)
            self.assertEqual(list(match_kana_kashira(aligned)), [])
