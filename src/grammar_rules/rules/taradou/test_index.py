import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.taradou.index import RULE, match_taradou
from src.grammar_rules.rules.tara.index import RULE as TARA
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TaradouTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '休んだらどう？': 'だらどう', '病院に行ったらどうですか。': 'たらどうですか',
            '行かなかったらどう？': 'たらどう', '読んだらどう': 'だらどう',
            '行ったらどうする？': None, '行ったらどうなる？': None,
            '行ったらどうだった？': None, '行ったらどうです。': None,
            '行ったらどうかな。': None, '行ったら どう？': None,
            'たらどう？': None, 'どうですか。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_taradou(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_card_example(self):
        source = sentence('今日はゆっくり休んだらどう？', [
            tok('今日はゆっくり', '今日', ('名詞',)),
            tok('休ん', '休む', ('動詞', '一般', '*', '*', '五段', '連用形-撥音便')),
            tok('だら', 'だ', ('助動詞', '*', '*', '*', '助動詞-タ', '仮定形-一般')),
            tok('どう', 'どう', ('副詞', '*', '*', '*', '*', '*')),
            tok('？', '?', ('補助記号', '句点'))])
        stage = GrammarStep(rules=(RULE, TARA))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['だら', 'どう'])
        words = {'休む': object()}
        stage.add_cards(words)
        self.assertIn('休む', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'だらどう')
