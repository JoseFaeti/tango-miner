import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import YOTEIDA_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.yoteida.index import match_yoteida
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class YoteidaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '明日行く予定だ。': '予定だ',
            '明日行く予定です。': '予定です',
            '明日行く予定だった。': '予定だった',
            '明日行く予定でした。': '予定でした',
            '明日行かない予定だ。': '予定だ',
            '明日は出発の予定だ。': '予定だ',
            '予定だ。': None,
            '明日の予定だ。': '予定だ',
            '予定を変更する。': None,
            '行く予定はない。': None,
            '行く予定なら知らせて。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_yoteida(aligned)],
                                 [] if expected is None else [expected], raw)

    def test_card_reserves_expression_and_preserves_verb(self):
        source = sentence('私は明日学校へ行く予定だ。', [
            tok('私は明日学校へ', '学校', ('名詞',)),
            tok('行く', '行く', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('予定', '予定', ('名詞', '普通名詞')),
            tok('だ', 'だ', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['予定', 'だ'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[YOTEIDA_EXPRESSION].frequency, 2)
        self.assertEqual(words[YOTEIDA_EXPRESSION].sentences[0].surface_form, '予定だ')
