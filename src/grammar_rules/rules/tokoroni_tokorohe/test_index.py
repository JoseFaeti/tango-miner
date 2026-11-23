import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.tatokoro.index import RULE as TATOKORO
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.tokoroni_tokorohe.index import (
    RULE,
    match_tokoroni_tokorohe,
)
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, TOKORONI_TOKOROHE_EXPRESSION


class TokoroniTokoroheTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '出かけるところに電話が来た。': 'ところに',
            '出発するところに電話が来た。': 'ところに',
            '食べているところへ友達が来た。': 'ところへ',
            '帰ったところに雨が降った。': 'ところに',
            '困っていたところへ助けが来た。': 'ところへ',
            '忙しいところにすみません。': None,
            '静かなところに住む。': None,
            '静かだったところに住む。': None,
            'あんなたかいところにあってはとれぬ！': None,
            'でもあんな遠い所にわざわざ船を置く理由もないし。': None,
            'せっかくこんな広い所にいるんだからなんかしようぜ。': None,
            '学校のところへ行く。': None,
            '昨日行った所へ戻る。': '所へ',
            'ところに来た。': None,
            '帰るところを見た。': None,
            '帰るところ に来た。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tokoroni_tokorohe(aligned)],
                                 [] if expected is None else [expected], raw)

    def test_card_reservation_and_overlapping_tatokoro(self):
        source = sentence('帰ったところに来た。', [
            tok('帰っ', '帰る', ('動詞', '一般', '*', '*', '五段-ラ行', '連用形-促音便')),
            tok('た', 'た', ('助動詞', '*', '*', '*', '助動詞-タ', '連体形-一般')),
            tok('ところ', 'ところ', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('来た。', '来る', ('動詞',))])
        stage = GrammarStep(rules=(TATOKORO, RULE))
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['ところ', 'に'])
        words = {'帰る': object()}
        stage.add_cards(words)
        self.assertEqual(words[TOKORONI_TOKOROHE_EXPRESSION].frequency, 2)
        self.assertIn(TOKORONI_TOKOROHE_EXPRESSION, GRAMMAR_EXPRESSIONS)
        self.assertIn(TATOKORO.expression, words)
        self.assertIn('帰る', words)
