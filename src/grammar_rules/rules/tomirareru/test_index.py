import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.tomirareru.index import RULE, match_tomirareru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TomirareruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '事故が原因だと見られる。': 'と見られる',
            '被害が拡大したとみられた。': 'とみられた',
            '今後増えると見られます。': 'と見られます',
            '彼が犯人と見られました。': 'と見られました',
            '事故が原因であるものと見られる。': None,
            '彼が来ると 見られる。': None,
            '彼が来ると見る。': None,
            '彼が来ると見られない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tomirareru(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('彼は明日来ると見られる。', [
            tok('彼は明日', '彼', ('名詞',)),
            tok('来る', '来る', ('動詞', '*', '*', '*', '*', '終止形-一般')),
            tok('と', 'と', ('助詞', '格助詞')), tok('見', '見る', ('動詞',)),
            tok('られる', 'られる', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['と', '見', 'られる'])
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'と見られる')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
