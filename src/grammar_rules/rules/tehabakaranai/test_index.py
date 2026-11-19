import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.tehabakaranai.index import RULE, match_tehabakaranai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TehabakaranaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '公言してはばからない。': 'てはばからない',
            '公言して憚らなかった。': 'て憚らなかった',
            '叫んではばからない。': 'ではばからない',
            '言ってはばかりませんでした。': 'てはばかりませんでした',
            '言ってはばからなくて。': 'てはばからなく',
            '言ってはばからなければ。': 'てはばからなければ',
            '人目をはばからない。': None,
            '言ってはばかった。': None,
            '言ってはばからなかっ。': None,
            '言って はばからない。': None,
            'はばからない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tehabakaranai(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_card_keep_verb(self):
        parts = [tok('彼は', '彼', ('代名詞',)),
                 tok('言っ', '言う', ('動詞', '一般', '*', '*', '*', '連用形-促音便')),
                 tok('て', 'て', ('助詞', '接続助詞')),
                 tok('はばから', 'はばかる', ('動詞',)), tok('ない', 'ない', ('助動詞',)),
                 tok('。', '。', ('補助記号',))]
        source = sentence('彼は言ってはばからない。', parts)
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['て', 'はばから', 'ない'])
        words = {'言う': object()}
        stage.add_cards(words)
        self.assertIn(RULE, RULES)
        self.assertIn('言う', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'てはばからない')
