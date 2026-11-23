import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, TOKOROO_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.tokoroo.index import RULE, match_tokoroo
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TokorooTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            'お忙しいところをありがとうございます。': 'ところを',
            '帰るところを呼び止めた。': 'ところを',
            '寝ているところを起こされた。': 'ところを',
            '困っていたところを助けられた。': 'ところを',
            '危ないところを助けてくれて、ありがとうございました…！': 'ところを',
            '盗んでいるところを捕まえた。': 'ところを',
            '忙しいところを邪魔した。': 'ところを',
            'まさか大魔王の城が崩れるところを見られるとは！': None,
            'しかし…恥ずかしいところを見せてしまったようじゃな。': None,
            '静かなところを探す。': None,
            '昨日行った所を見た。': None,
            'ところを見た。': None,
            '帰るところに来た。': None,
            '帰るところ を見た。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tokoroo(aligned)],
                                 [] if expected is None else [expected], raw)

    def test_card_reservation(self):
        source = sentence('彼が帰るところを呼び止めた。', [
            tok('彼が', '彼', ('名詞',)),
            tok('帰る', '帰る', ('動詞', '一般', '*', '*', '五段-ラ行', '連体形-一般')),
            tok('ところ', 'ところ', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('呼び止めた。', '呼び止める', ('動詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['ところ', 'を'])
        words = {'帰る': object()}
        stage.add_cards(words)
        self.assertEqual(words[TOKOROO_EXPRESSION].frequency, 2)
        self.assertIn(TOKOROO_EXPRESSION, GRAMMAR_EXPRESSIONS)
        self.assertIn('帰る', words)
