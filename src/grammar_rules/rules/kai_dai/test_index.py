import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import KAI_DAI_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.kai_dai.index import RULE, match_kai_dai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KaiDaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行くかい？': 'かい', '元気かい？': 'かい', 'そうかい。': 'かい',
            '何だい？': 'だい', 'どこへ行くんだい？': 'だい', 'いいかい？': 'かい',
            '赤い貝。': None, '何回？': None, 'ちょうだい。': None,
            '行くかいな。': None, '行くか い。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kai_dai(aligned)],
                                 [] if expected is None else [expected])

    def test_integration(self):
        source = sentence('明日はあの場所へ行くかい。', [
            tok('明日はあの場所へ', '場所', ('名詞',)), tok('行く', '行く', ('動詞',)),
            tok('か', 'か', ('助詞', '終助詞')), tok('い', 'い', ('助詞', '終助詞')),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        reserved = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source, source])))
        self.assertEqual([t['surface'] for t in reserved.data[0].tokens
                          if t.get('grammar_reserved')], ['か', 'い'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertIn(RULE, RULES)
        self.assertEqual(words[KAI_DAI_EXPRESSION].frequency, 2)
        self.assertEqual(words[KAI_DAI_EXPRESSION].sentences[0].surface_form, 'かい')
