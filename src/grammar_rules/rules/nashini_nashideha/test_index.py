import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.nashini_nashideha.index import (
    RULE,
    match_nashini_nashideha,
)
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NashiniNashidehaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '努力なしに成功しない。': 'なしに',
            '許可なしに入る。': 'なしに',
            '努力無しに進む。': '無しに',
            '努力なしでは成功しない。': 'なしでは',
            '彼の助けなしでは無理だ。': 'なしでは',
            '梨に触る。': None, 'なしでは困る。': None,
            '努力 なしに成功する。': None, '努力なし に成功する。': None,
            '努力なしで成功する。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nashini_nashideha(aligned)],
                                 [expected] if expected else [])

    def test_reservation_and_card_identity(self):
        parts = [tok('努力', '努力', ('名詞',)), tok('なし', 'なし', ('名詞',)),
                 tok('で', 'で', ('助詞',)), tok('は', 'は', ('助詞',)),
                 tok('成功', '成功', ('名詞',)), tok('はずだ', 'はずだ', ('名詞',))]
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(
            stage.process(Artifact([sentence('努力なしでは成功はずだ', parts)])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['なし', 'で', 'は'])
        words = {'努力': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'なしでは')
        self.assertIn('努力', words)
