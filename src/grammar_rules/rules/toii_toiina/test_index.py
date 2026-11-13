import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.toii_toiina.index import RULE, match_toii_toiina
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class ToiiToiinaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '明日晴れるといいな。': 'といいな', '雨が降らないといい。': 'といい',
            '天気がよいといいですね。': 'といい', '合格すると良いな。': 'と良いな',
            '彼といい友達になった。': None, '猫といい犬といい元気だ。': None,
            '行くと言いました。': None, '晴れるとよかった。': None,
            '晴れると いいな。': None, 'といいな。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toii_toiina(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_example(self):
        source = sentence('明日晴れるといいな。', [
            tok('明日', '明日', ('名詞',)),
            tok('晴れる', '晴れる', ('動詞', '一般', '*', '*', '下一段', '終止形-一般')),
            tok('と', 'と', ('助詞', '接続助詞')),
            tok('いい', '良い', ('形容詞',)), tok('な', 'な', ('助詞', '終助詞')),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['と', 'いい', 'な'])
        words = {'晴れる': object()}
        stage.add_cards(words)
        self.assertIn('晴れる', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'といいな')
