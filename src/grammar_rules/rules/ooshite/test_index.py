import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.ooshite.index import RULE, match_ooshite
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class OoshiteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        from sudachipy import dictionary, tokenizer
        analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        cases = {
            '病を押して出席した。': 'を押して',
            '病気を押して働いた。': 'を押して',
            '風邪を押して出勤した。': 'を押して',
            '高熱を押して参加した。': 'を押して',
            '怪我を押して試合に出た。': 'を押して',
            'けがをおして出場した。': 'をおして',
            '親の反対を押して結婚した。': 'を押して',
            '無理を押してお願いした。': 'を押して',
            '悪天候を押して出発した。': 'を押して',
            '困難を押して進んだ。': 'を押して',
            'ボタンを押して開ける。': None,
            '背中を押して励ました。': None,
            'ドアを押して入る。': None,
            '印を押してください。': None,
            '病を推して出席した。': None,
            '病を押した。': None,
            '反対を押し切って進む。': None,
            '押して進む。': None,
            '病を 押して出席した。': None,
            '病、を押して出席した。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ooshite(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('病を押して出席した。', [
            tok('病', '病', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('押し', '押す', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('出席した。', '出席', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '押し', 'て'])
        self.assertFalse(data.data[0].tokens[0].get('grammar_reserved'))
        words = {'病': object()}
        stage.add_cards(words)
        self.assertIn('病', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を押して')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
