import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.niatatsute_nisaishite.index import (
    RULE,
    match_niatatsute_nisaishite,
)
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NiatatsuteNisaishiteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '入学にあたって準備する。': 'にあたって',
            '出発に当たって挨拶する。': 'に当たって',
            '出発に当たっての注意事項。': 'に当たって',
            '入学にあたっての準備。': 'にあたって',
            '始めるにあたって注意する。': 'にあたって',
            '開会に際して挨拶する。': 'に際して',
            '契約するに際して確認する。': 'に際して',
            '開会に際しての挨拶。': 'に際して',
            '壁に当たって跳ね返る。': None,
            '仕事にあたっている。': None,
            'とにかくいろんな人にあたってゼシカのゆくえをさぐるでがす。': None,
            'あなたたちは水の洞窟へいき水の巫女の警護にあたってください。': None,
            '以上、４名にて∠審査にあたってもらいます': None,
            '開始にあたって準備する。': 'にあたって',
            '試験を受けるにあたって、しっかり準備した。': 'にあたって',
            '出発に際して、注意事項を説明します。': 'に際して',
            '審査に際して準備する。': 'に際して',
            '審査にあたって準備する。': 'にあたって',
            '警護にあたって準備する。': 'にあたって',
            '壁にぶつかって倒れる。': None,
            '開会を際して挨拶する。': None,
            '開会に 際して挨拶する。': None,
            '開会に際する問題。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_niatatsute_nisaishite(tokens)],
                                 [] if expected is None else [expected], tokens)

    def test_reservation_and_identity(self):
        source = sentence('開会に際して挨拶する。', [
            tok('開会', '開会', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('際し', '際する', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('挨拶する。', '挨拶', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '際し', 'て'])
        words = {'開会': object()}
        stage.add_cards(words)
        self.assertIn('開会', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に際して')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)

    def test_niatatsute_reservation_keeps_preceding_noun_and_no(self):
        source = sentence('出発に当たっての準備。', [
            tok('出発', '出発', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('当たっ', '当たる', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('の', 'の', ('助詞',)), tok('準備。', '準備', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '当たっ', 'て'])
        words = {'出発': object()}
        stage.add_cards(words)
        self.assertIn('出発', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に当たって')
