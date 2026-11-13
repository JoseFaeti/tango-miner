import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.kara_node.index import RULE as CAUSAL_RULE
from src.grammar_rules.rules.karakoso.index import RULE, match_karakoso
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KarakosoTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '好きだからこそ言う。': True,
            '努力したからこそ成功した。': True,
            '高いからこそ大切にする。': True,
            '学生ですからこそ分かる。': True,
            '駅からこそ歩こう。': False,
            '食べてからこそ行こう。': False,
            '好きだから言う。': False,
            '好きだから こそ言う。': False,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_karakoso(aligned)],
                                 ['からこそ'] if expected else [])

    def test_cards_and_reservation(self):
        source = sentence('好きだからこそ言います。', [
            tok('好き', '好き', ('形状詞', '一般')),
            tok('だ', 'だ', ('助動詞', '*', '*', '*', '助動詞-ダ', '終止形-一般')),
            tok('から', 'から', ('助詞', '接続助詞')),
            tok('こそ', 'こそ', ('助詞', '係助詞')),
            tok('言います。', '言う', ('動詞',))])
        stage = GrammarStep(rules=(RULE, CAUSAL_RULE))
        artifact = stage.process(Artifact([source]))
        words = {'好き': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'からこそ')
        self.assertIn(CAUSAL_RULE.expression, words)
        self.assertIn('好き', words)
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens
                          if t.get('grammar_reserved')], ['から', 'こそ'])
