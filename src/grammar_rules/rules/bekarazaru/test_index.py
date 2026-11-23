import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.bekarazaru.index import RULE, match_bekarazaru
from src.grammar_rules.rules.bekarazu.index import RULE as GENERAL
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class BekarazaruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行くべからざる場所。': 'べからざる', '許すべからざる行為。': 'べからざる',
            'するべからざること。': 'べからざる', '行くべからず。': None,
            'べからざる場所。': None, '食べからざる。': None,
            '行く べからざる場所。': None, '行くべから ざる場所。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_bekarazaru(aligned)],
                                 [] if expected is None else [expected])

    def test_overlap_reservation_and_card(self):
        source = sentence('行くべからざる場所。', [
            tok('行く', '行く', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('べから', 'べし', ('助動詞',)), tok('ざる', 'ぬ', ('助動詞',)),
            tok('場所', '場所', ('名詞',)), tok('。', '。', ('補助記号',))])
        self.assertIn(RULE, RULES)
        stage = GrammarStep(rules=(GENERAL, RULE))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['べから', 'ざる'])
        words = {'行く': object()}
        stage.add_cards(words)
        for rule in (GENERAL, RULE):
            self.assertEqual(words[rule.expression].sentences[0].surface_form, 'べからざる')
        self.assertIn('行く', words)
