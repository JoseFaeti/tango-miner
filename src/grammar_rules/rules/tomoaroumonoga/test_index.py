import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.tomoaroumonoga.index import RULE, match_tomoaroumonoga
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TomoaroumonogaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '先生ともあろう者がそんなことをするとは。': 'ともあろう者が',
            '彼ともあろうものが失敗した。': 'ともあろうものが',
            '友とあろうものが来た。': None,
            '先生ともあろう者は立派だ。': None,
            '先生ともある者が来た。': None,
            '先生とも あろう者が来た。': None,
            'ともあろう者が来た。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tomoaroumonoga(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_card_identity(self):
        source = sentence('先生ともあろう者が失敗した。', [
            tok('先生', '先生', ('名詞',)), tok('と', 'と', ('助詞',)),
            tok('も', 'も', ('助詞',)), tok('あろう', 'ある', ('動詞',)),
            tok('者', '者', ('名詞',)), tok('が', 'が', ('助詞',)),
            tok('失敗した。', '失敗', ('名詞',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['と', 'も', 'あろう', '者', 'が'])
        self.assertFalse(result.data[0].tokens[0].get('grammar_reserved'))
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ともあろう者が')
