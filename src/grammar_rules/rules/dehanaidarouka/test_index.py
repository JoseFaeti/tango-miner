import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.darouka.index import RULE as DAROUKA
from src.grammar_rules.rules.dehanaidarouka.index import RULE, match_dehanaidarouka
from src.grammar_rules.rules.janai_dewanai.index import RULE as NEGATIVE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class DehanaidaroukaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼は学生ではないだろうか。': 'ではないだろうか',
            '彼が来るのではないだろうか。': 'ではないだろうか',
            'そうではないだろうかと考える。': 'ではないだろうか',
            '学生ではないか。': None, '学生じゃないだろうか。': None,
            '学生ではないでしょうか。': None, '学生ではなかっただろうか。': None,
            '学生ではないだろう。': None, '学生ではない だろうか。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_dehanaidarouka(tokens)],
                                 [] if expected is None else [expected])

    def test_card_reservation_and_overlap(self):
        source = sentence('彼は学生ではないだろうか。', [
            tok('彼は', '彼', ('代名詞',)), tok('学生', '学生', ('名詞',)),
            tok('で', 'だ', ('助動詞',)), tok('は', 'は', ('助詞',)),
            tok('ない', 'ない', ('形容詞',)), tok('だろう', 'だ', ('助動詞',)),
            tok('か', 'か', ('助詞', '終助詞')), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=tuple(r for r in RULES if r in (RULE, DAROUKA, NEGATIVE)))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['で', 'は', 'ない', 'だろう', 'か'])
        words = {'学生': object()}
        stage.add_cards(words)
        self.assertIn('学生', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ではないだろうか')
        self.assertEqual(words[DAROUKA.expression].sentences[0].surface_form, 'だろうか')
        self.assertNotIn(NEGATIVE.expression, words)
