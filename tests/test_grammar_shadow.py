import json
import tempfile
import unittest
from pathlib import Path

from src.GrammarShadowCLI import build_rows
from src.GrammarShadow import analyze_sentence
from src.TokenizedSentence import TokenizedSentence
from src.Artifact import Artifact
from src.steps.GrammarShadowStep import GrammarShadowStep


def tok(surface, lemma=None, pos=('名詞',)):
    return {'surface': surface, 'lemma': lemma or surface, 'pos': pos}


class GrammarShadowTests(unittest.TestCase):
    def test_repeated_spans_and_components_remain_distinct(self):
        sentence = TokenizedSentence('んだ、んだ', 'tag', Path('source'),
                                     [tok('ん'), tok('だ'), tok('、'), tok('んだ')])
        row = analyze_sentence(sentence, 0)
        proposals = row['proposals']
        self.assertEqual([p['span'] for p in proposals], [[0, 2], [3, 5]])
        self.assertNotEqual(proposals[0]['id'], proposals[1]['id'])
        self.assertEqual(len(proposals[0]['component_token_ids']), 2)
        self.assertTrue(all(p['status'] == 'ambiguous' for p in proposals))

    def test_controls_are_not_claimed_correct(self):
        sentence = TokenizedSentence('大事なものの名前', None, Path('source'),
                                     [tok('大事'), tok('な'), tok('もの'), tok('の'), tok('名前')])
        row = analyze_sentence(sentence, 0)
        self.assertEqual(row['proposals'][0]['construction_id'], 'concessive_monono')
        self.assertEqual(row['proposals'][0]['status'], 'ambiguous')

    def test_failed_alignment_does_not_guess(self):
        sentence = TokenizedSentence('読んでいる', None, Path('source'), [tok('読ん'), tok('いる')])
        row = analyze_sentence(sentence, 0)
        self.assertTrue(row['alignment_error'])
        self.assertEqual(row['proposals'], [])
        self.assertIsNone(row['raw_sentence'])

    def test_forms_and_original_tokens_are_preserved(self):
        tokens = [tok('読ん'), tok('で'), tok('いる'), tok('、'), tok('してる')]
        sentence = TokenizedSentence('読んでいる、してる', None, Path('source'), tokens)
        row = analyze_sentence(sentence, 0)
        self.assertEqual([p['surface'] for p in row['proposals']], ['でいる', 'てる'])
        self.assertEqual([t['surface'] for t in tokens], ['読ん', 'で', 'いる', '、', 'してる'])
        self.assertEqual(len(row['proposals'][1]['component_token_ids']), 1)

    def test_sample_ids_and_errors(self):
        sample = {'x': {'text': 'んだ', 'partition': 'development', 'group_id': 'g'}}
        rows = build_rows(sample, {'x': {'error': 'failed'}})
        self.assertEqual(rows[0]['error'], 'failed')
        with self.assertRaises(ValueError):
            build_rows(sample, {})

    def test_split_explanatory_and_te_iru_evidence(self):
        sentence = TokenizedSentence('読んでいるんだ', None, Path('source'), [
            tok('読ん', '読む', ('動詞', '一般')),
            tok('で', 'で', ('助詞', '接続助詞')),
            tok('いる', 'いる', ('動詞', '非自立可能')),
            tok('ん', 'ん', ('助詞', '準体助詞')),
            tok('だ', 'だ', ('助動詞', '*'))])
        row = analyze_sentence(sentence, 0)
        self.assertIn('connective_plus_iru_analysis', row['proposals'][0]['evidence'])
        self.assertIn('nominalizer_plus_copula_analysis', row['proposals'][1]['evidence'])
        self.assertTrue(all(p['status'] == 'ambiguous' for p in row['proposals']))

    def test_split_noun_particle_remains_available_for_review(self):
        sentence = TokenizedSentence('大事なものの名前', None, Path('source'), [
            tok('大事'), tok('な'), tok('もの', pos=('名詞', '普通名詞')),
            tok('の', pos=('助詞', '格助詞')), tok('名前')])
        p = analyze_sentence(sentence, 0)['proposals'][0]
        self.assertIn('noun_plus_particle_analysis_not_disambiguating', p['evidence'])

    def test_raw_offsets_and_identity_do_not_depend_on_sample_order(self):
        sample = {'x': {'text': 'んだ', 'partition': 'development', 'group_id': 'g'},
                  'y': {'text': 'ものの', 'partition': 'holdout', 'group_id': 'h'}}
        analyses = {'x': {'tokens': [tok('んだ')]}, 'y': {'tokens': [tok('ものの')]}}
        first = {r['id']: r for r in build_rows(sample, analyses)}
        second = {r['id']: r for r in build_rows(dict(reversed(list(sample.items()))), analyses)}
        self.assertEqual(first['x']['sentence_id'], second['x']['sentence_id'])
        self.assertEqual(first['x']['proposals'][0]['raw_span'], [0, 2])
        self.assertEqual(first['x']['raw_mapping_status'], 'identity_raw_input')

    def test_inflection_and_contraction_preserve_surface_and_components(self):
        sentence = TokenizedSentence('読んでいた、見てた', None, Path('source'), [
            tok('読ん', '読む', ('動詞', '一般')),
            tok('で', 'で', ('助詞', '接続助詞')),
            tok('い', 'いる', ('動詞', '非自立可能')),
            tok('た', 'た', ('助動詞', '*')), tok('、'),
            tok('見', '見る', ('動詞', '非自立可能')),
            tok('て', 'てる', ('助動詞', '*')),
            tok('た', 'た', ('助動詞', '*'))])
        proposals = analyze_sentence(sentence, 0)['proposals']
        self.assertEqual([p['surface'] for p in proposals], ['でいた', 'てた'])
        self.assertIn('inflected_iru_plus_auxiliary_analysis', proposals[0]['evidence'])
        self.assertIn('contracted_inflected_auxiliary_analysis', proposals[1]['evidence'])
        self.assertEqual([len(p['component_token_ids']) for p in proposals], [3, 2])

    def test_lexical_iru_and_deru_do_not_trigger_inflected_rule(self):
        sentence = TokenizedSentence('人がいた、祭りに出た', None, Path('source'), [
            tok('人'), tok('が'), tok('い', 'いる', ('動詞', '非自立可能')),
            tok('た', 'た', ('助動詞', '*')), tok('、'), tok('祭り'), tok('に'),
            tok('出', '出る', ('動詞', '一般')), tok('た', 'た', ('助動詞', '*'))])
        self.assertEqual(analyze_sentence(sentence, 0)['proposals'], [])

    def test_polite_suffixes_keep_complete_supported_surface(self):
        def aux(surface, lemma):
            return tok(surface, lemma, ('助動詞', '*'))
        cases = [
            ('読んでいました', [tok('読ん'), tok('で', pos=('助詞', '接続助詞')),
                           tok('い', 'いる'), aux('まし', 'ます'), aux('た', 'た')], 'でいました'),
            ('見てました', [tok('見'), aux('て', 'てる'), aux('まし', 'ます'), aux('た', 'た')], 'てました'),
            ('読んでいませんでした', [tok('読ん'), tok('で', pos=('助詞', '接続助詞')),
                                   tok('い', 'いる'), aux('ませ', 'ます'), aux('ん', 'ぬ'),
                                   aux('でし', 'です'), aux('た', 'た')], 'でいませんでした'),
            ('読んでいまして', [tok('読ん'), tok('で', pos=('助詞', '接続助詞')),
                           tok('い', 'いる'), aux('まし', 'ます'), tok('て', pos=('助詞', '接続助詞'))], 'でいまして'),
            ('読んでいますから', [tok('読ん'), tok('で', pos=('助詞', '接続助詞')),
                              tok('い', 'いる'), aux('ます', 'ます'), tok('から')], 'でいます'),
            ('読んでいませんでし', [tok('読ん'), tok('で', pos=('助詞', '接続助詞')),
                               tok('い', 'いる'), aux('ませ', 'ます'), aux('ん', 'ぬ'),
                               aux('でし', 'です')], 'でいません'),
        ]
        for text, tokens, expected in cases:
            with self.subTest(text=text):
                sentence = TokenizedSentence(text, None, Path('source'), tokens)
                proposals = analyze_sentence(sentence, 0)['proposals']
                self.assertEqual([p['surface'] for p in proposals], [expected])
                self.assertEqual(proposals[0]['status'], 'ambiguous')

    def test_suffix_not_extended_across_gaps_or_wrong_pos(self):
        sentence = TokenizedSentence('読んでいまし た', None, Path('source'), [
            tok('読ん'), tok('で', pos=('助詞', '接続助詞')), tok('い', 'いる'),
            tok('まし', 'ます', ('助動詞', '*')), tok('た', 'た', ('助動詞', '*'))])
        self.assertEqual(analyze_sentence(sentence, 0)['proposals'][0]['surface'], 'でいまし')

    def test_pipeline_shadow_stage_preserves_input_and_writes_provenance(self):
        sentence = TokenizedSentence('読んでいた', None, Path('source'), [
            tok('読ん', '読む', ('動詞', '一般')), tok('で', 'で', ('助詞', '接続助詞')),
            tok('い', 'いる', ('動詞', '非自立可能')), tok('た', 'た', ('助動詞', '*'))])
        artifact = Artifact([sentence])
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'shadow.jsonl'
            self.assertIs(GrammarShadowStep(path).process(artifact), artifact)
            row = json.loads(path.read_text(encoding='utf-8'))
            self.assertEqual(row['proposals'][0]['surface'], 'でいた')
            self.assertIsNone(row['proposals'][0]['raw_span'])
            self.assertEqual(len(row['rule_sha256']), 64)
            self.assertEqual(sentence.tokens[1]['surface'], 'で')


if __name__ == '__main__':
    unittest.main()

