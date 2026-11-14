import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nai.index import RULE as NAI
from src.grammar_rules.rules.nisouinai.index import RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NisouinaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼は犯人に相違ない。': 'に相違ない',
            '雨が降るに相違ない。': 'に相違ない',
            '寒いに相違ない。': 'に相違ない',
            '静かに相違なかった。': 'に相違なかった',
            'それに相違なくて驚いた。': 'に相違なくて',
            '彼に相違なければ困る。': 'に相違なければ',
            '彼に相違ないです。': 'に相違ない',
            '彼にそういない。': None,
            '相違がない。': None,
            '彼に相違がない。': None,
            '彼に相違はない。': None,
            '彼に 相違ない。': None,
            '彼に相違なかっ。': None,
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
        source = sentence('彼は犯人に相違ない。', [
            tok('彼は', '彼', ('代名詞',)),
            tok('犯人', '犯人', ('名詞', '普通名詞', '一般')),
            tok('に', 'に', ('助詞', '格助詞')),
            tok('相違', '相違', ('名詞', '普通名詞', 'サ変可能')),
            tok('ない', 'ない', ('形容詞', '非自立可能')),
            tok('。', '。', ('補助記号',))])
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        stage = GrammarStep(rules=(RULE, NAI))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '相違', 'ない'])
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved'))
        words = {'犯人': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に相違ない')
        self.assertIn('犯人', words)
