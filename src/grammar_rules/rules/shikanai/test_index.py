import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.shikanai.index import RULE, match_shikanai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class ShikanaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'水しかない。': True, '一人しか来ない。': True,
                 'それしか買わなかった。': True, '水しかありません。': True,
                 '待つしかない。': True, '鹿がいない。': False,
                 'しかし行かない。': False, '水しか飲むが食べない。': False,
                 '水しか。食べない。': False}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_shikanai(aligned)],
                                 ['しか'] if expected else [])

    def test_reservation_and_lexical_evidence(self):
        source = sentence('私は水しか飲まない。', [
            tok('私は', '私', ('代名詞',)), tok('水', '水', ('名詞',)), tok('しか', 'しか', ('助詞', '副助詞')),
            tok('飲ま', '飲む', ('動詞',)), tok('ない', 'ない', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')], ['しか'])
        words = {'水': object(), '飲む': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'しか')
        self.assertIn('水', words)
        self.assertIn('飲む', words)
