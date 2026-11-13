import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.okagede.index import RULE, match_okagede
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class OkagedeTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '手伝ってくれたおかげで助かった。': 'おかげで',
            '天気がいいおかげで楽しめた。': 'おかげで',
            '先生のおかげで合格した。': 'おかげで',
            '元気なおかげで働ける。': 'おかげで',
            'そのおかげで助かった。': 'おかげで',
            '君のお陰で助かった。': 'お陰で',
            'おかげで助かった。': None,
            '先生のおかげを忘れない。': None,
            '先生のおかげでなく自分でやった。': None,
            '先生の おかげで助かった。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_okagede(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_card_evidence(self):
        source = sentence('先生のおかげで合格した。', [
            tok('先生', '先生', ('名詞',)), tok('の', 'の', ('助詞', '格助詞')),
            tok('おかげ', 'おかげ', ('名詞',)), tok('で', 'で', ('助詞', '格助詞')),
            tok('合格した。', '合格する', ('動詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['おかげ', 'で'])
        words = {'先生': object()}
        stage.add_cards(words)
        self.assertIn('先生', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'おかげで')
