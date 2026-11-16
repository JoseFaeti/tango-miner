import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.okinjienai.index import RULE, match_okinjienai
from src.grammar_rules.rules.uru_enai.index import RULE as URU_RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class OkinjienaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '驚きを禁じ得ない。': 'を禁じ得ない', '涙を禁じ得なかった。': 'を禁じ得なかった',
            '怒りを禁じえません。': 'を禁じえません', '悲しみを禁じ得ず。': 'を禁じ得ず',
            '怒りを禁じ得なくて。': 'を禁じ得なく', '驚きを禁じ得なければ。': 'を禁じ得なければ',
            '笑いを禁じ得ぬ。': 'を禁じ得ぬ', '禁じ得ない。': None,
            '驚きを禁じ得た。': None, '驚き を禁じ得ない。': None,
            '驚きを禁じ得なかっ。': None, '利益を得ない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_okinjienai(aligned)],
                                 [] if expected is None else [expected], raw)

    def test_reservation_and_precedence(self):
        parts = [tok('彼の言葉に', '言葉', ('名詞',)), tok('驚き', '驚き', ('名詞',)), tok('を', 'を', ('助詞',)),
                 tok('禁じ', '禁じる', ('動詞', '*', '*', '*', '*', '連用形')),
                 tok('得', '得る', ('動詞',)), tok('ない', 'ない', ('助動詞',))]
        source = sentence('彼の言葉に驚きを禁じ得ない。', parts + [tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, URU_RULE))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '禁じ', '得', 'ない'])
        words = {'驚き': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を禁じ得ない')
        self.assertIn(URU_RULE.expression, words)
        self.assertIn('驚き', words)
