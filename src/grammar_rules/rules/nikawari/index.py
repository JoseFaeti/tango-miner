"""Noun + に代わり: formal substitution or representation."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nikawari(tokens):
    for i in range(1, len(tokens) - 2):
        noun, particle, verb, following = tokens[i - 1:i + 3]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and particle['surface'] == 'に' and particle['pos'][:1] == ['助詞']
                and verb['surface'] in ('代わり', 'かわり')
                and verb['lemma'] in ('代わる', 'かわる') and verb['pos'][:1] == ['動詞']
                and adjacent(noun, particle) and adjacent(particle, verb)
                and following['pos'][:1] != ['助動詞']
                and following['surface'] not in ('。', '！', '？', '!', '?')):
            yield particle['span'][0], verb['span'][1]


RULE = GrammarRule(order=0, id='nikawari', expression='〜に代わり',
                   reading='にかわり', meaning='in place of; on behalf of',
                   match=match_nikawari)
RULES = (RULE,)
