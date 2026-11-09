import unittest
import json
import os
import tempfile
from unittest.mock import patch
from pathlib import Path

from src.TokenizedSentence import TokenizedSentence
from src.Artifact import Artifact
from src.Pipeline import Pipeline
from src.steps.BuildWordStatsStep.index import build_word_stats
from src.steps.MergeTokensStep.index import combine_tokens_jmdict, MergeTokensStep, get_jmdict, _is_split_kana_noun


def token(surface, lemma, pos):
    return {'surface': surface, 'lemma': lemma, 'base_form': lemma,
            'reading': surface, 'pos': pos}


class Dictionary:
    def lookup_word(self, word):
        return [{'senses': [{'pos': ['expression']}]}] if word == 'ないで' else []

    def get_best_entry(self, word):
        return word

    def get_reading(self, entry, word):
        return word


class ProvenanceTests(unittest.TestCase):
    def test_moushi_noun_boundaries_and_controls(self):
        from src.steps.TokenizeStep.index import get_tokenizer, TOKENIZER_MODE, sudachi_node_to_dict

        cases = [
            ('しかし、きみ達、ずいぶん体力をしょうもうしているな？', '消耗', ['しょう', 'もう']),
            ('しかし、きみ達も、ずいぶん体力をしょうもうしているな？', '消耗', ['しょう', 'もう']),
            ('ところが、とつぜん皇帝からわかいしようという、もうしでがあったの！', '申し出', ['もう', 'し', 'で']),
        ]
        for sentence, lemma, components in cases:
            with self.subTest(sentence=sentence):
                raw = [sudachi_node_to_dict(n) for n in get_tokenizer().tokenize(sentence, TOKENIZER_MODE)]
                result = combine_tokens_jmdict(raw, Dictionary())
                item = next(t for t in result if t['lemma'] == lemma)
                self.assertEqual([c.surface for c in item['components']], components)
                self.assertNotIn('もうし', [t['lemma'] for t in result])
                if lemma == '消耗':
                    next(t for t in raw if t['surface'] == 'て')['grammar_reserved'] = True
                    reserved = combine_tokens_jmdict(raw, Dictionary())
                    self.assertIn('消耗', [t['lemma'] for t in reserved])
                    self.assertTrue(next(t for t in reserved if t['surface'] == 'て')['grammar_reserved'])
        for sentence in ('もうしばらく待ってください。', 'そうもうしました。', 'もうしで終わりです。'):
            with self.subTest(sentence=sentence):
                raw = [sudachi_node_to_dict(n) for n in get_tokenizer().tokenize(sentence, TOKENIZER_MODE)]
                result = combine_tokens_jmdict(raw, Dictionary())
                self.assertFalse(any(t.get('analysis_rule') == 'moushi_noun_boundary' for t in result))

    def test_karai_source_boundaries_and_adjective_controls(self):
        from src.steps.TokenizeStep.index import get_tokenizer, TOKENIZER_MODE, sudachi_node_to_dict

        cases = [
            ('さっさと虹色の枝ってのをいただいてこの暑い国からズラかろうぜ。', False, 'ズラかる'),
            ('新しい花、何か買ってくっからさ。', False, None),
            ('たったう、カルバナイルからさエリカに大変です。', False, None),
            ('この料理はからい。', True, None),
            ('この料理はからかった。', True, None),
            ('この料理のからさが好きだ。', True, None),
        ]
        for sentence, expected, recovered in cases:
            with self.subTest(sentence=sentence):
                raw = [sudachi_node_to_dict(n) for n in get_tokenizer().tokenize(sentence, TOKENIZER_MODE)]
                result = combine_tokens_jmdict(raw, Dictionary())
                self.assertEqual(any(t['lemma'] == 'からい' for t in result), expected)
                if recovered:
                    item = next(t for t in result if t['lemma'] == recovered)
                    self.assertEqual([c.surface for c in item['components']], ['ズラ', 'かろう'])

    def test_split_noun_rule_requires_surface_and_pos_evidence(self):
        noun = [{'senses': [{'pos': ['noun (common) (futsuumeishi)']}]}]
        first = token('さい', 'さい', ['名詞', '普通名詞'])
        second = token('だん', 'だん', ['接尾辞', '名詞的'])
        self.assertTrue(_is_split_kana_noun([first, second], noun))
        cases = [
            ([token('さい', 'さい', ['名詞', '固有名詞']), second], noun),
            ([first, token('だん', 'だん', ['名詞', '普通名詞'])], noun),
            ([first, token('だん', 'だる', ['接尾辞', '名詞的'])], noun),
            ([token('さ', 'さ', ['名詞', '普通名詞']), token('い', 'い', ['接尾辞', '名詞的'])], noun),
            ([token('祭', '祭', ['名詞', '普通名詞']), second], noun),
            ([first, second], [{'senses': [{'pos': ['verb']}] }]),
            ([first], noun),
            ([first, second, second], noun),
        ]
        for tokens, definitions in cases:
            with self.subTest(tokens=tokens, definitions=definitions):
                self.assertFalse(_is_split_kana_noun(tokens, definitions))

    def test_dictionary_cache_uses_requested_path(self):
        with patch("src.steps.MergeTokensStep.index._jmdict", None), \
             patch("src.steps.MergeTokensStep.index._jmdict_path", None), \
             patch("src.steps.MergeTokensStep.index.JMDict") as dictionary:
            first = get_jmdict("first.xml")
            self.assertIs(first, get_jmdict("first.xml"))
            get_jmdict("second.xml")
            self.assertEqual(dictionary.call_count, 2)
            self.assertEqual(dictionary.call_args_list[0].args[0], Path("first.xml").resolve())
            self.assertEqual(dictionary.call_args_list[1].args[0], Path("second.xml").resolve())

    def test_default_dictionary_uses_current_configuration(self):
        original = Path.cwd()
        with tempfile.TemporaryDirectory() as directory:
            try:
                os.chdir(directory)
                Path("config.toml").write_text('[dictionary]\njmdict_path = "data/custom.xml"\n')
                with patch("src.steps.MergeTokensStep.index._jmdict", None), \
                     patch("src.steps.MergeTokensStep.index._jmdict_path", None), \
                     patch("src.steps.MergeTokensStep.index.JMDict") as dictionary:
                    get_jmdict()
                    dictionary.assert_called_once_with(Path("data/custom.xml").resolve())
                    get_jmdict("explicit.xml")
                    self.assertEqual(dictionary.call_args.args[0], Path("explicit.xml").resolve())
            finally:
                os.chdir(original)

    def test_configured_lookahead_keeps_source_and_component_evidence(self):
        class ThreeTokenDictionary(Dictionary):
            def lookup_word(self, word):
                if word in {"ないで", "ないでね"}:
                    return [{"senses": [{"pos": ["expression"]}]}]
                return []
        raw = [token("ない", "ない", ["助動詞", "*"]),
               token("で", "で", ["助詞", "*"]),
               token("ね", "ね", ["助詞", "*"])]
        sentence = TokenizedSentence("ないでね", "tag", Path("source"), raw)
        with patch("src.steps.MergeTokensStep.index.get_jmdict", return_value=ThreeTokenDictionary()):
            short = Pipeline([MergeTokensStep(max_lookahead=2)]).run(Artifact([sentence])).data[0].tokens
            long = Pipeline([MergeTokensStep(max_lookahead=3)]).run(Artifact([sentence])).data[0].tokens
        self.assertEqual([item["surface"] for item in short], ["ないで", "ね"])
        self.assertEqual([item["surface"] for item in long], ["ないでね"])
        self.assertEqual([item.surface for item in long[0]["components"]], ["ない", "で", "ね"])
        self.assertEqual(long[0]["lemma"], "ないでね")

    def test_multiple_boundary_repairs_in_one_lexical_run(self):
        raw = [token('ことし', 'ことし', ['名詞', '*']),
               token('て', 'て', ['助詞', '*']),
               token('で', 'だ', ['助動詞', '*']),
               token('すきっと', 'すきっと', ['副詞', '*']),
               token('。', '。', ['補助記号', '*'])]
        result = combine_tokens_jmdict(raw, Dictionary())
        self.assertEqual([item['surface'] for item in result],
                         ['こと', 'し', 'て', 'で', 'す', 'きっと', '。'])
        self.assertEqual(result[0]['components'][0].surface, 'ことし')
        self.assertEqual(result[5]['components'][0].surface, 'すきっと')

    def test_protected_boundaries_preserve_neighboring_merges_and_evidence(self):
        class BoundaryDictionary(Dictionary):
            def lookup_word(self, word):
                return [{'senses': [{'pos': ['expression']}]}] if len(word) >= 2 else []

        for boundary in ('desu_kitto_boundary', 'kara_sa_boundary', 'grammar_reserved'):
            with self.subTest(boundary=boundary):
                tokens = [token(char, char, ['名詞', '*']) for char in '甲乙丙丁戊']
                if boundary == 'grammar_reserved':
                    tokens[2][boundary] = True
                else:
                    tokens[2]['analysis_rule'] = boundary
                rejected = []
                merged = combine_tokens_jmdict(tokens, BoundaryDictionary(), rejected=rejected)
                self.assertEqual([item['surface'] for item in merged], ['甲乙', '丙', '丁戊'])
                self.assertEqual([part.surface for part in merged[0]['components']], ['甲', '乙'])
                self.assertEqual([part.surface for part in merged[2]['components']], ['丁', '戊'])
                self.assertEqual(rejected, [])

    def test_synthetic_merge_boundary_keeps_omitted_particle_fallback(self):
        class ExpressionDictionary(Dictionary):
            def lookup_word(self, word):
                return [{'senses': [{'pos': ['expression']}]}] if word == '甲を行く' else []

            def get_best_entry(self, word):
                return self.lookup_word(word)[0]

        noun = token('甲', '甲', ['名詞', '*'])
        verb = token('行く', '行く', ['動詞', '*'])
        noun['analysis_rule'] = 'kara_sa_boundary'
        merged = combine_tokens_jmdict([noun, verb], ExpressionDictionary())
        self.assertEqual([item['lemma'] for item in merged], ['甲を行く'])
        self.assertEqual([part.surface for part in merged[0]['components']], ['甲', '行く'])
        noun['grammar_reserved'] = True
        unmerged = combine_tokens_jmdict([noun, verb], ExpressionDictionary())
        self.assertEqual([item['surface'] for item in unmerged], ['甲', '行く'])

    def test_diagnostic_output_is_ordered_and_replaced_only_on_success(self):
        sentences = [TokenizedSentence(text, 'tag', Path('source'), [])
                     for text in ('first', 'second')]

        def merge(tokens, dictionary, *, max_lookahead, rejected):
            rejected.append({'reason': 'test'})
            return tokens

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'rejected.jsonl'
            output.write_text('previous', encoding='utf-8')
            stage = MergeTokensStep(diagnostics_path=output)
            stage.start_time = None
            with patch('src.steps.MergeTokensStep.index.get_jmdict', return_value=Dictionary()), \
                 patch('src.steps.MergeTokensStep.index.combine_tokens_jmdict', side_effect=merge):
                stage.process(Artifact(sentences))
            events = [json.loads(line) for line in output.read_text(encoding='utf-8').splitlines()]
            self.assertEqual([(event['sentence_index'], event['sentence'], event['reason'])
                              for event in events], [(0, 'first', 'test'), (1, 'second', 'test')])
            self.assertFalse(output.with_suffix('.jsonl.tmp').exists())
            before = output.read_bytes()
            with patch('src.steps.MergeTokensStep.index.get_jmdict', return_value=Dictionary()), \
                 patch('src.steps.MergeTokensStep.index.combine_tokens_jmdict', side_effect=RuntimeError('failure')):
                with self.assertRaises(RuntimeError):
                    stage.process(Artifact(sentences))
            self.assertEqual(output.read_bytes(), before)

    def test_merge_retains_immutable_original_components(self):
        raw = [token('ない', 'ない', ['助動詞', '*']), token('で', 'で', ['助詞', '*'])]
        merged = combine_tokens_jmdict(raw, Dictionary())
        self.assertEqual(len(merged), 1)
        self.assertEqual([(c.surface, c.lemma, c.reading, c.pos) for c in merged[0]['components']],
                         [('ない', 'ない', 'ない', ('助動詞', '*')), ('で', 'で', 'で', ('助詞', '*'))])
        raw[0]['pos'][0] = 'changed'
        self.assertEqual(merged[0]['components'][0].pos[0], '助動詞')

    def test_aggregation_counts_distinct_analyses_without_changing_candidates(self):
        merged = combine_tokens_jmdict([token('ない', 'ない', ['助動詞', '*']),
                                         token('で', 'で', ['助詞', '*'])], Dictionary())[0]
        standalone = token('ないで', 'ないで', ['名詞', '*'])
        sentences = [TokenizedSentence('ないでください。', 'tag', Path('a'), [t])
                     for t in (merged, merged, standalone)]
        words, _ = build_word_stats(sentences)
        self.assertEqual(set(words), {'ないで'})
        self.assertEqual(words['ないで'].frequency, 3)
        self.assertEqual(sorted(words['ないで'].component_counts.values()), [1, 2])
        self.assertEqual(words['ないで'].pos[0], 'exp')
        self.assertEqual(words['ないで'].tags, {'tag'})

    def test_repeated_lemma_preserves_each_analysis_for_example_selection(self):
        from src.steps.AttachSentencesStep.index import attach_sentences
        raw = [token('ようやく', 'ようやく', ['副詞', '*']),
               token('ようやく', 'ようやく', ['名詞', '*'])]
        words, segments = build_word_stats([
            TokenizedSentence('ようやくようやくという言葉を使いました。', 'tag', Path('a'), raw)])
        stats = words['ようやく']
        stats.score = 500
        self.assertEqual(stats.frequency, 2)
        self.assertEqual(len(segments[0].lemma_occurrences['ようやく']), 2)
        adverb = next(c for c in stats.component_counts if c[0].pos[0] == '副詞')
        stats.definition_components = {adverb}
        attach_sentences(words, segments)
        self.assertEqual(len(stats.sentences), 1)
        self.assertEqual(stats.sentences[0].span, (0, 4))
        self.assertIn("<span class='highlight'>ようやく</span>ようやく", stats.sentences[0].to_html())

    def test_unmerged_token_keeps_original_evidence(self):
        raw = token('風花', '風花', ['名詞', '固有名詞'])
        merged = combine_tokens_jmdict([raw], Dictionary())
        self.assertEqual(merged[0]['pos'], ['名詞', '固有名詞'])
        eligible = token('風花', '風花', ['名詞', '普通名詞'])
        words, _ = build_word_stats([TokenizedSentence('風花がいる。', '', Path('a'), [eligible])])
        components, = words['風花'].component_counts
        self.assertEqual(components[0].pos, ('名詞', '普通名詞'))
        self.assertEqual(components[0].surface, '風花')


if __name__ == '__main__':
    unittest.main()
