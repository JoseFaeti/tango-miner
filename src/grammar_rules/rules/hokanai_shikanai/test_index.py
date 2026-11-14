import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.hokanai_shikanai.index import RULE, match_hokanai_shikanai
from src.grammar_rules.rules.shikanai.index import RULE as ONLY_RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class HokanaiShikanaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '待つしかない。': 'しかない', '帰るしかなかった。': 'しかなかった',
            '待つしかありません。': 'しかありません', '行くほかない。': 'ほかない',
            '行くほかはなかった。': 'ほかはなかった', '行く他はありません。': '他はありません',
            '水しかない。': None, '一人しか来ない。': None, '他はない。': None,
            '待ったしかない。': None, '行くほかにもある。': None, '鹿がいない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_hokanai_shikanai(aligned)],
                                 [] if expected is None else [expected])

    def test_overlap_and_lexical_evidence(self):
        source = sentence('私はここで待つしかない。', [
            tok('私はここで', '私', ('代名詞',)),
            tok('待つ', '待つ', ('動詞', '一般', '*', '*', '五段', '終止形-一般')),
            tok('しか', 'しか', ('助詞', '副助詞')),
            tok('ない', 'ない', ('形容詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, ONLY_RULE))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        words = {'待つ': object()}
        stage.add_cards(words)
        self.assertIn('待つ', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'しかない')
        self.assertEqual(words[ONLY_RULE.expression].sentences[0].surface_form, 'しか')
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['しか', 'ない'])
