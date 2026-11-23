import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.karashite.index import RULE, match_karashite
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KarashiteTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '名前からして怪しい。': True, '彼の態度からして違う。': True,
            '私からして分からない。': True, '朝から仕事をしている。': False,
            '食べてからしている。': False, '名前から して怪しい。': False,
            '名前からした判断だ。': False,
            '話し方からして怪しい。': True,
            '彼が来たことからして意外だ。': True,
            '先生からして規則を守らない。': True,
            'うんほいきた、剣をちょっとばかしあずからしてもらうよっ！': False,
            '剣を預からしてもらう。': False,
            '剣をあずからせてもらう。': False,
            'あずからして怪しい。': True,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_karashite(aligned)],
                                 ['からして'] if expected else [])

    def test_complete_verb_token_is_not_a_particle_boundary(self):
        for surface, lemma in [('預からし', '預からす'), ('あずからし', 'あずからす')]:
            with self.subTest(surface=surface):
                text = '剣を' + surface + 'てもらう。'
                source = sentence(text, [
                    tok('剣', '剣', ('名詞', '普通名詞')),
                    tok('を', 'を', ('助詞', '格助詞')),
                    tok(surface, lemma, ('動詞', '一般')),
                    tok('て', 'て', ('助詞', '接続助詞')),
                    tok('もらう', 'もらう', ('動詞', '非自立可能')),
                    tok('。', '。', ('補助記号', '句点'))])
                stage = GrammarStep(rules=(RULE,))
                artifact = stage.process(Artifact([source]))
                words = {}
                stage.add_cards(words)
                self.assertNotIn(RULE.expression, words)
                ExcludeGrammarTokensStep(stage).process(artifact)
                self.assertFalse(any(t.get('grammar_reserved')
                                     for t in artifact.data[0].tokens))

    def test_card_and_reservation_preserve_noun(self):
        source = sentence('名前からして怪しい。', [
            tok('名前', '名前', ('名詞', '普通名詞')),
            tok('から', 'から', ('助詞', '格助詞')),
            tok('し', 'する', ('動詞', '非自立可能')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('怪しい。', '怪しい', ('形容詞',))])
        stage = GrammarStep(rules=(RULE,))
        artifact = stage.process(Artifact([source]))
        words = {'名前': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'からして')
        self.assertIn('名前', words)
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens
                          if t.get('grammar_reserved')], ['から', 'し', 'て'])


if __name__ == '__main__':
    unittest.main()
