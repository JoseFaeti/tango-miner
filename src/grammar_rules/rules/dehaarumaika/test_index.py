import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.dehaarumaika.index import RULE, match_dehaarumaika
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class DehaarumaikaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼は犯人ではあるまいか。': 'ではあるまいか',
            '犯人じゃあるまいか。': 'じゃあるまいか',
            '問題ではあるまいかと思う。': 'ではあるまいか',
            'そこにはあるまいか。': None,
            '学校ではあるまいか。': 'ではあるまいか',
            '犯人ではあるまい。': None,
            '犯人ではあるまいかしら。': None,
            '犯人では あるまいか。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_dehaarumaika(tokens)],
                                 [] if expected is None else [expected])

    def test_card_reservation_and_lexical_evidence(self):
        source = sentence('犯人ではあるまいか。', [
            tok('犯人', '犯人', ('名詞',)), tok('で', 'で', ('助詞',)),
            tok('は', 'は', ('助詞',)), tok('ある', 'ある', ('動詞',)),
            tok('まい', 'まい', ('助動詞',)), tok('か', 'か', ('助詞', '終助詞')),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertFalse(result.data[0].tokens[0].get('grammar_reserved'))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['で', 'は', 'ある', 'まい', 'か'])
        words = {'犯人': object()}
        stage.add_cards(words)
        self.assertIn('犯人', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ではあるまいか')
