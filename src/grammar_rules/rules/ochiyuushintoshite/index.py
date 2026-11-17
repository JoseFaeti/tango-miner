"""Noun + を中心とする: centered on or mainly involving something."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_ochiyuushintoshite(tokens):
    for i in range(1, len(tokens) - 3):
        noun, wo, center, to, verb = tokens[i - 1:i + 4]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or wo['surface'] != 'を' or wo['pos'][:1] != ['助詞']
                or center['surface'] != '中心' or center['lemma'] != '中心'
                or center['pos'][:1] != ['名詞']
                or to['surface'] != 'と' or to['pos'][:1] != ['助詞']
                or verb['lemma'] != 'する' or verb['pos'][:1] != ['動詞']
                or not all(adjacent(a, b) for a, b in zip(
                    (noun, wo, center, to), (wo, center, to, verb)))):
            continue
        if verb['surface'] == 'する':
            yield wo['span'][0], verb['span'][1]
        elif verb['surface'] == 'し' and i + 4 < len(tokens):
            ending = tokens[i + 4]
            if (adjacent(verb, ending)
                    and ((ending['surface'] == 'て' and ending['pos'][:1] == ['助詞'])
                         or (ending['surface'] == 'た' and ending['pos'][:1] == ['助動詞']))):
                yield wo['span'][0], ending['span'][1]


RULE = GrammarRule(id='ochiyuushintoshite', expression='〜を中心として',
                   reading='をちゅうしんとして', meaning='centered on; mainly involving',
                   match=match_ochiyuushintoshite)
RULES = (RULE,)
