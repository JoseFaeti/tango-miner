import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.dehanaika.index import RULE, match_dehanaika
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.youdehanaika.index import RULE as INVITATION


class DehanaikaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼は学生ではないか。': 'ではないか',
            'そうではないか。': 'ではないか',
            '彼が来るのではないか。': 'ではないか',
            '学生ではありませんか。': 'ではありませんか',
            '学生ではないかと聞いた。': 'ではないか',
            '学生ではない。': None, '学生じゃないか。': None,
            '行こうではないか。': None, '駅ではないかばんを買った。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_dehanaika(tokens)],
                                 [] if expected is None else [expected])

    def test_card_and_reservation(self):
        source = sentence('彼は学生ではないか。', [
            tok('彼は', '彼', ('代名詞',)), tok('学生', '学生', ('名詞',)),
            tok('で', 'だ', ('助動詞',)), tok('は', 'は', ('助詞',)),
            tok('ない', 'ない', ('形容詞',)), tok('か', 'か', ('助詞', '終助詞')),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, INVITATION))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['で', 'は', 'ない', 'か'])
        words = {'学生': object()}
        stage.add_cards(words)
        self.assertIn('学生', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ではないか')
        self.assertIn(RULE, RULES)
