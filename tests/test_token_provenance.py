import unittest
from unittest.mock import patch
from pathlib import Path

from src.TokenizedSentence import TokenizedSentence
from src.Artifact import Artifact
from src.Pipeline import Pipeline
from src.steps.BuildWordStatsStep import build_word_stats
from src.steps.MergeTokensStep import combine_tokens_jmdict, MergeTokensStep, get_jmdict, _is_split_kana_noun


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
        with patch("src.steps.MergeTokensStep._jmdict", None), \
             patch("src.steps.MergeTokensStep._jmdict_path", None), \
             patch("src.steps.MergeTokensStep.JMDict") as dictionary:
            first = get_jmdict("first.xml")
            self.assertIs(first, get_jmdict("first.xml"))
            get_jmdict("second.xml")
            self.assertEqual(dictionary.call_count, 2)
            self.assertEqual(dictionary.call_args_list[0].args[0], Path("first.xml").resolve())
            self.assertEqual(dictionary.call_args_list[1].args[0], Path("second.xml").resolve())

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
        with patch("src.steps.MergeTokensStep.get_jmdict", return_value=ThreeTokenDictionary()):
            short = Pipeline([MergeTokensStep(max_lookahead=2)]).run(Artifact([sentence])).data[0].tokens
            long = Pipeline([MergeTokensStep(max_lookahead=3)]).run(Artifact([sentence])).data[0].tokens
        self.assertEqual([item["surface"] for item in short], ["ないで", "ね"])
        self.assertEqual([item["surface"] for item in long], ["ないでね"])
        self.assertEqual([item.surface for item in long[0]["components"]], ["ない", "で", "ね"])
        self.assertEqual(long[0]["lemma"], "ないでね")

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
