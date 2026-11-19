import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.bekarazu.index import RULE, match_bekarazu
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class BekarazuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '立ち入るべからず。': 'べからず', 'するべからず。': 'べからず',
            'すべからず。': 'べからず', '忘るべからず。': 'べからず',
            '行くべからざる場所。': 'べからざる', 'べからず。': None,
            '食べからず。': None, '行く べからず。': None, '行くべから ず。': None,
            '行くべきだ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_bekarazu(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_card_example(self):
        source = sentence('明日は行くべからず。', [
            tok('明日は', '明日', ('名詞',)),
            tok('行く', '行く', ('動詞', '一般', '*', '*', '五段', '終止形-一般')),
            tok('べから', 'べし', ('助動詞',)), tok('ず', 'ず', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        self.assertIn(RULE, RULES)
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['べから', 'ず'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'べからず')
        self.assertIn('行く', words)
