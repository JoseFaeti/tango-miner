import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.tatokoro.index import RULE as TATOKORO
from src.grammar_rules.rules.tatokorode.index import RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.tokorode.index import RULE as TOKORODE


class TatokorodeTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        from src.grammar_rules.alignment import aligned_tokens
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '今さら謝ったところで許されない。': 'ところで',
            '急いだところで間に合わない。': 'ところで',
            '昨日行った所で会った。': '所で',
            'ところで話がある。': None,
            '静かなところで休む。': None,
            '帰るところで会った。': None,
            '謝ったところ で許されない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in RULE.match(aligned)],
                                 [] if expected is None else [expected])

    def test_identity_reservation_and_overlap(self):
        source = sentence('急いだところで間に合わない。', [
            tok('急い', '急ぐ', ('動詞',)), tok('だ', 'だ', ('助動詞',)),
            tok('ところ', 'ところ', ('名詞',)), tok('で', 'で', ('助詞',)),
            tok('間に合わない。', '間に合う', ('動詞',))])
        stage = GrammarStep(rules=(TATOKORO, TOKORODE, RULE))
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['ところ', 'で'])
        words = {'急ぐ': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ところで')
        self.assertIn(TOKORODE.expression, words)
        self.assertIn(TATOKORO.expression, words)
        self.assertIn('急ぐ', words)
        self.assertIn(RULE, RULES)
