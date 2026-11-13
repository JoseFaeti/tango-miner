import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.tame.index import RULE, match_tame
from src.grammar_rules.rules.tame_ni.index import RULE as TAME_NI_RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TameTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨が降ったため、中止した。': 'ため',
            '病気のため、休んだ。': 'ため',
            'その為、遅れた。': '為',
            '日本へ行くために勉強する。': None,
            '健康のためになる。': None,
            'ためになる。': None,
            '健康のための運動。': None,
            '雨が降った ため、中止した。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tame(tokens)],
                                 [] if expected is None else [expected])

    def test_card_reservation_and_separate_tame_ni(self):
        source = sentence('病気のため、休んだ。', [
            tok('病気', '病気', ('名詞',)), tok('の', 'の', ('助詞', '格助詞')),
            tok('ため', 'ため', ('名詞',)), tok('、', '、', ('補助記号',)),
            tok('休んだ。', '休む', ('動詞',))])
        stage = GrammarStep(rules=(RULE, TAME_NI_RULE))
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['ため'])
        words = {'病気': object()}
        stage.add_cards(words)
        self.assertIn('病気', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ため')
        self.assertNotIn(TAME_NI_RULE.expression, words)
