import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.zutomo.index import RULE, match_zutomo
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class ZutomoTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '言わずとも分かる。': True, '知らずとも構わない。': False,
            'せずとも大丈夫だ。': True, 'ずっと友達だ。': False,
            '地図と友達。': False, 'ずとも分かる。': False,
            '言わず とも分かる。': False, '言わ ずとも分かる。': False,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_zutomo(aligned)],
                                 ['ずとも'] if expected else [])

    def test_reservation_and_example(self):
        parts = [tok('言わ', '言う', ('動詞', '一般', '*', '*', '*', '未然形-一般')),
                 tok('ず', 'ず', ('助動詞',)), tok('とも', 'とも', ('助詞',)), tok('分かる', '分かる', ('動詞',)), tok('はずだ', 'はずだ', ('名詞',))]
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(
            stage.process(Artifact([sentence('言わずとも分かるはずだ', parts)])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ず', 'とも'])
        words = {'言う': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ずとも')
        self.assertIn('言う', words)
