import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import NAGARAMO_EXPRESSION, NAGARA_GRAMMAR_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.nagaramo.index import match_nagaramo
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NagaramoTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '知りながらも黙っていた。': True,
            '残念ながらも参加した。': True,
            '子供ながらも理解した。': True,
            '忙しいながらも頑張る。': True,
            '音楽を聴きながら歩く。': False,
            '生まれながらの才能もある。': False,
            '知りながらも。': False,
            '知りながらも。次の日に話す。': False,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nagaramo(aligned)],
                                 ['ながらも'] if expected else [])

    def test_overlap_and_reservation(self):
        source = sentence('知りながらも何も言わずに黙っていた。', [
            tok('知り', '知る', ('動詞', '一般', '*', '*', '五段', '連用形-一般')),
            tok('ながら', 'ながら', ('助詞', '接続助詞')),
            tok('も', 'も', ('助詞', '係助詞')),
            tok('何も言わずに黙っていた。', '黙る', ('動詞',))])
        artifact = Artifact([source, source])
        stage = GrammarStep()
        stage.process(artifact)
        words = {'知る': object()}
        stage.add_cards(words)
        self.assertIn('知る', words)
        self.assertIn(NAGARAMO_EXPRESSION, words)
        self.assertNotIn(NAGARA_GRAMMAR_EXPRESSION, words)
        self.assertEqual(words[NAGARAMO_EXPRESSION].frequency, 2)
        self.assertEqual(words[NAGARAMO_EXPRESSION].sentences[0].surface_form, 'ながらも')
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens
                          if t.get('grammar_reserved')], ['ながら', 'も'])
