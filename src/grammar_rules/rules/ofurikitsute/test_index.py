import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.ofurikitsute.index import RULE, match_ofurikitsute
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class OfurikitsuteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        from sudachipy import dictionary, tokenizer

        analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        cases = {
            '反対を振り切って出発した。': 'を振り切って',
            '追手を振り切って逃げた。': 'を振り切って',
            '不安をふりきって進んだ。': 'をふりきって',
            '迷いを振りきって走る。': 'を振りきって',
            '腕を振り切って投げた。': 'を振り切って',
            '反対を振り切っても進む。': 'を振り切って',
            '反対を振り切った。': None,
            '反対を振り切らず進む。': None,
            '手を振って別れた。': None,
            '紙を切って捨てた。': None,
            '振り切って逃げた。': None,
            'を振り切って逃げた。': None,
            '反対、を振り切って進む。': None,
            '反対を 振り切って進む。': None,
            '反対 を振り切って進む。': None,
            '反対を振り切っ、て進む。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ofurikitsute(tokens)],
                                 [] if expected is None else [expected], raw)

    def test_reservation_identity_and_attached_word(self):
        source = sentence('反対を振り切って進んだ。', [
            tok('反対', '反対', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('振り切っ', '振り切る', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('進んだ。', '進む', ('動詞',))])
        stage = GrammarStep()
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '振り切っ', 'て'])
        words = {'反対': object()}
        stage.add_cards(words)
        self.assertIn('反対', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を振り切って')
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
