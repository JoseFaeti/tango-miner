import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.janai_dewanai.index import RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class JanaiDewanaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '学生じゃない。': 'じゃない', '学生ではない。': 'ではない',
            '静かじゃなかった。': 'じゃなかった', '学生ではなかったです。': 'ではなかったです',
            '学生ではありません。': 'ではありません', '学生じゃありませんでした。': 'じゃありませんでした',
            '確かに聞こえた…まだ来るべきところではない…と。': 'ではない',
            'ここはあなたがたの来るようなところではありません。': 'ではありません',
            'おもしろ半分に立ち入るところではありませんよ。': 'ではありません',
            '本を読んではない。': None, 'お金がない。': None,
            '学生 じゃない。': None, '学生ではなかっ': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in RULE.match(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_card(self):
        source = sentence('彼は学生じゃないよ。', [
            tok('彼は', '彼', ('代名詞',)), tok('学生', '学生', ('名詞',)),
            tok('じゃ', 'だ', ('助動詞',)), tok('ない', 'ない', ('形容詞',)),
            tok('よ。', 'よ', ('助詞',))])
        self.assertIn(RULE, RULES)
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['じゃ', 'ない'])
        words = {'学生': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'じゃない')
        self.assertIn('学生', words)

    def test_reported_sentences_survive_expression_precedence(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = (
            ('確かに聞こえた…まだ来るべきところではない…と。', 'ではない'),
            ('ここはあなたがたの来るようなところではありません。', 'ではありません'),
            ('おもしろ半分に立ち入るところではありませんよ。', 'ではありません'),
        )
        for text, expected in cases:
            with self.subTest(text=text):
                tokens = [dict(tok(m.surface(), m.dictionary_form(), m.part_of_speech()),
                               base_form=m.dictionary_form(), reading=m.reading_form())
                          for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                stage = GrammarStep(prefer_expressions=True)
                result = ExcludeGrammarTokensStep(stage).process(
                    stage.process(Artifact([sentence(text, tokens)])))
                matches = [(a, b) for rule_id, a, b in stage.spans.get(0, ())
                           if rule_id == RULE.id]
                self.assertEqual([text[a:b] for a, b in matches], [expected])
                self.assertTrue(any(t.get('grammar_reserved') and t['surface'] == 'で'
                                    for t in result.data[0].tokens))
                self.assertTrue(any(t['surface'] == 'ところ' and not t.get('grammar_reserved')
                                    for t in result.data[0].tokens))
