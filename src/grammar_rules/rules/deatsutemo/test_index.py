import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.deatsute.index import RULE as DEATSUTE
from src.grammar_rules.rules.deatsutemo.index import RULE, match_deatsutemo
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class DeatsutemoTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '友人であっても油断できない。': 'であっても',
            '静かであっても安心できない。': 'であっても',
            '学校で会っても話さない。': None,
            '学生であって、教師でもある。': None,
            '学生であってはならない。': None,
            '学生であって も困る。': None,
            '学生で あっても困る。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_deatsutemo(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_overlap(self):
        source = sentence('友人であっても困る。', [
            tok('友人', '友人', ('名詞',)), tok('で', 'だ', ('助動詞',)),
            tok('あっ', 'ある', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('も', 'も', ('助詞',)), tok('困る。', '困る', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['で', 'あっ', 'て', 'も'])
        words = {'友人': object()}
        stage.add_cards(words)
        self.assertIn('友人', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'であっても')
        self.assertEqual(words[DEATSUTE.expression].sentences[0].surface_form, 'であって')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
