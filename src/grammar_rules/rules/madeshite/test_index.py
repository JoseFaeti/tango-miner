import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.madeshite.index import RULE, match_madeshite
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MadeshiteTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '借金までして買った。': True, '嘘をついてまでして欲しい。': True,
            'そこまでして欲しい。': True, '夜まで仕事をしている。': False,
            '借金まで して買った。': False, '借金までした。': False,
            '借金までしない。': False, 'までして欲しい。': False,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_madeshite(aligned)],
                                 ['までして'] if expected else [])

    def test_card_and_reservation_preserve_noun(self):
        source = sentence('借金までして買った。', [
            tok('借金', '借金', ('名詞', '普通名詞')),
            tok('まで', 'まで', ('助詞', '副助詞')),
            tok('し', 'する', ('動詞', '非自立可能')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('買った。', '買う', ('動詞',))])
        stage = GrammarStep(rules=(RULE,))
        artifact = stage.process(Artifact([source]))
        words = {'借金': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'までして')
        self.assertIn('借金', words)
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens
                          if t.get('grammar_reserved')], ['まで', 'し', 'て'])


if __name__ == '__main__':
    unittest.main()
