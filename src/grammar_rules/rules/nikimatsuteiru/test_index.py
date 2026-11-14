import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nikimatsuteiru.index import RULE
from src.grammar_rules.rules.te_iru.index import RULE as TE_IRU
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NikimatsuteiruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼は犯人に決まっている。': 'に決まっている',
            '雨が降るに決まっている。': 'に決まっている',
            '寒いに決まっていた。': 'に決まっていた',
            '静かに決まっています。': 'に決まっています',
            '彼が勝つにきまっている。': 'にきまっている',
            '彼が勝つに決まっていない。': 'に決まっていない',
            '彼は犯人に決まってる。': None,
            '予定が決まっている。': None,
            '彼に決まった。': None,
            '彼に 決まっている。': None,
            '彼に決まって。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in RULE.match(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('彼は犯人に決まっている。', [
            tok('彼は', '彼', ('代名詞',)),
            tok('犯人', '犯人', ('名詞', '普通名詞', '一般')),
            tok('に', 'に', ('助詞', '格助詞')),
            tok('決まっ', '決まる', ('動詞', '一般', '*', '*', '五段-ラ行', '連用形-促音便')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('いる', 'いる', ('動詞', '非自立可能')),
            tok('。', '。', ('補助記号',))])
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        stage = GrammarStep(rules=(RULE, TE_IRU))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '決まっ', 'て', 'いる'])
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved'))
        words = {'犯人': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に決まっている')
        self.assertIn('犯人', words)
