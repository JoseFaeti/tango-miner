import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.nakushite.index import RULE, match_nakushite
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NakushiteTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '努力なくして成功はない。': True,
            '彼の協力なくしては成功しない。': True,
            '財布をなくして困った。': False,
            '物を無くしてしまった。': False,
            '努力 なくして成功はない。': False,
            'なくして成功はない。': False,
            '努力なしに成功はない。': False,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nakushite(aligned)],
                                 ['なくして'] if expected else [])

    def test_reservation_and_card_identity(self):
        parts = [tok('努力', '努力', ('名詞',)), tok('なく', 'ない', ('形容詞',)),
                 tok('し', 'する', ('動詞',)), tok('て', 'て', ('助詞',)),
                 tok('成功', '成功', ('名詞',)), tok('はずだ', 'はずだ', ('名詞',))]
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(
            stage.process(Artifact([sentence('努力なくして成功はずだ', parts)])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['なく', 'し', 'て'])
        words = {'努力': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'なくして')
        self.assertIn('努力', words)
