import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarShadow import aligned_tokens
from src.grammar_rules.rules.tame_ni import RULE, match_tame_ni
from src.grammar_rules.rules.tests.test_grammar_te_iru import sentence, tok


class TameNiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '日本へ行くために勉強する。': 'ために',
            '健康のために走る。': 'ために',
            '雨が降ったために中止した。': 'ために',
            '病気のために休んだ。': 'ために',
            'あなたの為に作った。': '為に',
            'ためになる。': None,
            'そのために困った。': 'ために',
            'その為に困った。': '為に',
            '読むために。': 'ために',
            '君の為に。': '為に',
            'ために。': None,
            '読むためになる。': 'ために',
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tame_ni(aligned)],
                                 [expected] if expected else [])

    def test_card_and_reservation_keep_attached_words(self):
        source = sentence('私は本を読むために勉強する。', [
            tok('私は本を', '本', ('名詞',)),
            tok('読む', '読む', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('ため', 'ため', ('名詞', '普通名詞')),
            tok('に', 'に', ('助詞', '格助詞')),
            tok('勉強する。', '勉強する', ('動詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['ため', 'に'])
        words = {'読む': object(), '勉強する': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ために')
        self.assertIn('読む', words)
        self.assertIn('勉強する', words)
