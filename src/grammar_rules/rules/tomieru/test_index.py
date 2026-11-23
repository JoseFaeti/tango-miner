import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.tomieru.index import RULE, match_tomieru
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class TomieruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼は疲れていると見える。': 'と見える',
            '雨が降ったと見えた。': 'と見えた',
            '彼は忙しいと見えます。': 'と見えます',
            '彼は元気だと見えました。': 'と見えました',
            '彼は疲れていると見えない。': 'と見えない',
            '彼は疲れていると見えなかった。': 'と見えなかった',
            '彼は疲れていると見えませんでした。': 'と見えませんでした',
            '彼は疲れていると見えて、帰った。': 'と見えて',
            '山が顔と見える。': None,
            '彼は疲れていると思える。': None,
            '彼は疲れていると 見える。': None,
            '彼は疲れていると見え。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tomieru(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('彼は疲れていると見える。', [
            tok('彼は', '彼', ('名詞',)),
            tok('疲れている', 'いる', ('動詞', '非自立可能', '*', '*', '下一段', '終止形-一般')),
            tok('と', 'と', ('助詞', '格助詞')),
            tok('見える', '見える', ('動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['と', '見える'])
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'と見える')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
