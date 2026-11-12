import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.jiyanaito_denaito.index import RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class JiyanaitoDenaitoTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '学生じゃないと入れない。': 'じゃないと',
            '学生でないと入れない。': 'でないと',
            '静かじゃないと眠れない。': 'じゃないと',
            '早く行こう。じゃないと遅れる。': 'じゃないと',
            '学生じゃないと言った。': None,
            '行かないと間に合わない。': None,
            '学生じゃない。': None,
            '本を読んでないと困る。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in RULE.match(aligned)],
                                 [] if expected is None else [expected], ascii(raw))

    def test_reservation_and_card(self):
        source = sentence('学生じゃないと入れない。', [
            tok('学生', '学生', ('名詞',)), tok('じゃ', 'だ', ('助動詞',)),
            tok('ない', 'ない', ('形容詞',)), tok('と', 'と', ('助詞', '接続助詞')),
            tok('入れない。', '入る', ('動詞',))])
        self.assertIn(RULE, RULES)
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['じゃ', 'ない', 'と'])
        words = {'学生': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'じゃないと')
        self.assertIn('学生', words)
