import unittest
from pathlib import Path

from src.TokenizedSentence import TokenizedSentence
from src.steps.BuildWordStatsStep import build_word_stats
from src.steps.MergeTokensStep import combine_tokens_jmdict


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
