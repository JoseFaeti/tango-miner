import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.seide.index import RULE, match_seide
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class SeideTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨が降ったせいで遅れた。': 'せいで',
            '忙しいせいで眠れない。': 'せいで',
            '雨のせいで遅れた。': 'せいで',
            '不便なせいで困った。': 'せいで',
            'そのせいで遅れた。': 'せいで',
            '君の所為で遅れた。': '所為で',
            'せいで遅れた。': None,
            '雨のせいにした。': None,
            '雨のせいでなく風のせいだ。': None,
            '雨の せいで遅れた。': None,
            '先生である。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_seide(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_card_evidence(self):
        source = sentence('大雨のせいで学校に遅れた。', [
            tok('大雨', '大雨', ('名詞',)), tok('の', 'の', ('助詞', '格助詞')),
            tok('せい', 'せい', ('名詞',)), tok('で', 'で', ('助詞', '格助詞')),
            tok('学校に遅れた。', '遅れる', ('動詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['せい', 'で'])
        words = {'大雨': object()}
        stage.add_cards(words)
        self.assertIn('大雨', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'せいで')
