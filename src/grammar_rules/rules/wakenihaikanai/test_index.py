import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.wakenihaikanai.index import RULE
from src.grammar_rules.rules.nai.index import RULE as NAI
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class WakenihaikanaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '今日は休むわけにはいかない。': 'わけにはいかない',
            '負ける訳には行かなかった。': '訳には行かなかった',
            '帰るわけにはいきませんでした。': 'わけにはいきませんでした',
            '逃げるわけにいかない。': 'わけにいかない',
            '行かないわけにはいかない。': None,
            '理由のわけにはいかない。': None,
            '帰るわけには行く。': None,
            '帰るわけ にはいかない。': None,
            '帰るわけにはいかなかっ。': None,
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
        source = sentence('休むわけにはいかない。', [
            tok('休む', '休む', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('わけ', 'わけ', ('名詞', '普通名詞', '一般')),
            tok('に', 'に', ('助詞', '格助詞')),
            tok('は', 'は', ('助詞', '係助詞')),
            tok('いか', 'いく', ('動詞', '非自立可能')),
            tok('ない', 'ない', ('助動詞',)), tok('。', '。', ('補助記号',))])
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        stage = GrammarStep(rules=(RULE, NAI))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['わけ', 'に', 'は', 'いか', 'ない'])
        self.assertFalse(result.data[0].tokens[0].get('grammar_reserved'))
        words = {'休む': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'わけにはいかない')
        self.assertIn('休む', words)
