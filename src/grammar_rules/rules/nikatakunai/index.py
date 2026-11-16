"""Literary expression for something readily imagined or inferred."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_suffix_end, next_matches


def match_nikatakunai(tokens):
    for i in range(1, len(tokens) - 2):
        before, ni, kata, negative = tokens[i - 1:i + 3]
        if (before['pos'][:1] not in (['名詞'], ['動詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or (kata['surface'], kata['lemma']) not in (('難く', '難い'), ('かたく', 'かたい'))
                or kata['pos'][:1] != ['形容詞']
                or not all(adjacent(a, b) for a, b in ((before, ni), (ni, kata), (kata, negative)))):
            continue
        end = negative_suffix_end(tokens, i + 2, pos='形容詞', continuative=True, conditional=True)
        if end is None:
            continue
        if next_matches(tokens, end, 'です', '助動詞', 'です'):
            end += 1
        yield ni['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='nikatakunai', expression='〜に難くない', reading='にかたくない',
                   meaning='not difficult to imagine or infer', match=match_nikatakunai)
RULES = (RULE,)
