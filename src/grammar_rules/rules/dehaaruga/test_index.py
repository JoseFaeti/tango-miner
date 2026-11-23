import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.dehaaruga.index import RULE, match_dehaaruga
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class DehaarugaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼は学生ではあるが、働いている。': 'ではあるが',
            '便利ではあるが、高い。': 'ではあるが',
            '学生じゃあるが、働いている。': 'じゃあるが',
            '学校ではあるが、遠い。': 'ではあるが',
            'そこにはあるが、見えない。': None,
            '学生ではないが、働いている。': None,
            '学生ではある。': None,
            '学生では あるが、働いている。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_dehaaruga(tokens)],
                                 [] if expected is None else [expected])

    def test_registry_reservation_and_lexical_evidence(self):
        source = sentence('学生ではあるが、働く。', [
            tok('学生', '学生', ('名詞',)), tok('で', 'で', ('助詞',)),
            tok('は', 'は', ('助詞',)), tok('ある', 'ある', ('動詞',)),
            tok('が', 'が', ('助詞', '接続助詞')), tok('、', '、', ('補助記号',)),
            tok('働く', '働く', ('動詞',)), tok('。', '。', ('補助記号',))])
        self.assertIn(RULE, RULES)
        stage = GrammarStep(rules=RULES)
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertFalse(result.data[0].tokens[0].get('grammar_reserved'))
        self.assertEqual([t['surface'] for t in result.data[0].tokens[1:5]
                          if t.get('grammar_reserved')], ['で', 'は', 'ある', 'が'])
        words = {'学生': object()}
        stage.add_cards(words)
        self.assertIn('学生', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ではあるが')
