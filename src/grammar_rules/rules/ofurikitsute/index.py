"""Object + を振り切って, including kana and split-verb spellings."""
import itertools

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_ofurikitsute(tokens):
    for i in range(1, len(tokens) - 2):
        previous, wo, verb = tokens[i - 1:i + 2]
        if (previous['pos'][:1] in (['補助記号'], ['空白'])
                or wo['surface'] != 'を' or wo['pos'][:1] != ['助詞']
                or verb['pos'][:1] != ['動詞']):
            continue
        end = i + 2
        if (verb['surface'], verb['lemma']) not in (
                ('振り切っ', '振り切る'), ('ふりきっ', 'ふりきる'),
                ('ふりきっ', '振り切る')):
            if (verb['surface'] not in ('振り', 'ふり')
                    or verb['lemma'] not in ('振る', 'ふる')
                    or end >= len(tokens)):
                continue
            cut = tokens[end]
            if (cut['surface'] not in ('切っ', 'きっ')
                    or cut['lemma'] not in ('切る', 'きる')
                    or cut['pos'][:1] != ['動詞']):
                continue
            end += 1
        if end >= len(tokens):
            continue
        te = tokens[end]
        parts = tokens[i - 1:end + 1]
        if (te['surface'] == 'て' and te['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in itertools.pairwise(parts))):
            yield wo['span'][0], te['span'][1]


RULE = GrammarRule(order=1, id='ofurikitsute', expression='〜を振り切って',
                   reading='をふりきって', meaning='shaking off; breaking free of',
                   match=match_ofurikitsute)
RULES = (RULE,)
