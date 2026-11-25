import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nikakawarinaku.index import RULE, match_nikakawarinaku
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NikakawarinakuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '天候にかかわりなく開催する。': 'にかかわりなく',
            '年齢に関わりなく参加できる。': 'に関わりなく',
            '性別に拘わりなく採用する。': 'に拘わりなく',
            '行くかどうかにかかわりなく知らせてください。': 'にかかわりなく',
            '参加するかしないかにかかわりなく連絡する。': 'にかかわりなく',
            '年齢や性別にかかわりなく参加できる。': 'にかかわりなく',
            '経験の有無にかかわりなく応募できる。': 'にかかわりなく',
            '年齢とはかかわりなく参加できる。': None,
            '年齢にかかわりはなく参加できる。': None,
            '事件に関わらずに暮らした。': None,
            '雨にもかかわらず出かけた。': None,
            '仕事に関わった。': None,
            '天候に かかわりなく開催する。': None,
            '年齢に関わりがなく困る。': None,
            '事件に関わりない生活だ。': None,
            '年齢に関わりなくなる。': None,
            '事件に関わりなく暮らす。': 'に関わりなく',
            'に関わりなく開催する。': None,
            '年齢、に関わりなく参加する。': None,
            '年齢に関わり なく参加する。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nikakawarinaku(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('天候にかかわりなく開催する。', [
            tok('天候', '天候', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('かかわり', 'かかわり', ('名詞',)), tok('なく', 'ない', ('形容詞',)),
            tok('開催する。', '開催', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'かかわり', 'なく'])
        words = {'天候': object()}
        stage.add_cards(words)
        self.assertIn('天候', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'にかかわりなく')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)

    def test_kanji_spelling_uses_existing_card_and_preserves_noun(self):
        source = sentence('年齢に関わりなく参加できる。', [
            tok('年齢', '年齢', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('関わり', '関わり', ('名詞',)), tok('なく', 'ない', ('形容詞',)),
            tok('参加できる。', '参加', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '関わり', 'なく'])
        words = {'年齢': object()}
        stage.add_cards(words)
        self.assertIn('年齢', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に関わりなく')

    def test_repeated_spellings_share_one_identity(self):
        text = '年齢にかかわりなく、性別に関わりなく参加する。'
        raw = [
            tok('年齢', '年齢', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('かかわり', 'かかわり', ('名詞',)), tok('なく', 'ない', ('形容詞',)),
            tok('、', '、', ('補助記号',)), tok('性別', '性別', ('名詞',)),
            tok('に', 'に', ('助詞',)), tok('関わり', '関わる', ('動詞',)),
            tok('なく', 'ない', ('形容詞',)), tok('参加する。', '参加', ('名詞',)),
        ]
        tokens, error = aligned_tokens(text, raw)
        self.assertIsNone(error)
        self.assertEqual([text[a:b] for a, b in match_nikakawarinaku(tokens)],
                         ['にかかわりなく', 'に関わりなく'])
        self.assertEqual([r for r in RULES if r.reading == 'にかかわりなく'], [RULE])
