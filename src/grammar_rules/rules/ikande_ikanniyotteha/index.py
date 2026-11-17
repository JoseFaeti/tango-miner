"""Noun + いかんで / いかんによって(は): depending on the circumstances."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

IKANDE_EXPRESSION = '〜いかんで / 〜いかんによっては'


def match_ikande(tokens):
    for i in range(1, len(tokens)):
        marker = tokens[i]
        previous = tokens[i - 1]
        if previous['surface'] == 'の' and previous['pos'][:1] == ['助詞']:
            if i < 2 or not adjacent(tokens[i - 2], previous):
                continue
            previous = tokens[i - 2]
        if (previous['pos'][:1] not in (['名詞'], ['代名詞'])
                or not adjacent(tokens[i - 1], marker)):
            continue
        if (marker['surface'] in ('如何で', 'いかんで')
                and marker['lemma'] == marker['surface'] and marker['pos'][:1] == ['副詞']):
            yield marker['span'][0], marker['span'][1]
            continue
        if (i + 1 >= len(tokens)
                or marker['surface'] not in ('いかん', '如何')
                or marker['lemma'] not in ('いかん', '如何')
                or marker['pos'][:1] != ['名詞']):
            continue
        following = tokens[i + 1]
        if not adjacent(marker, following) or following['pos'][:1] != ['助詞']:
            continue
        if following['surface'] == 'で':
            yield marker['span'][0], following['span'][1]
        elif following['surface'] == 'に' and i + 3 < len(tokens):
            verb, te = tokens[i + 2:i + 4]
            if (adjacent(following, verb) and adjacent(verb, te)
                    and verb['surface'] in ('よっ', '依っ', '拠っ')
                    and verb['lemma'] in ('よる', '依る', '拠る')
                    and verb['pos'][:1] == ['動詞']
                    and te['surface'] == 'て' and te['pos'][:1] == ['助詞']):
                end = te
                if (i + 4 < len(tokens) and adjacent(te, tokens[i + 4])
                        and tokens[i + 4]['surface'] == 'は'
                        and tokens[i + 4]['pos'][:1] == ['助詞']):
                    end = tokens[i + 4]
                yield marker['span'][0], end['span'][1]


RULE = GrammarRule(order=1, id='ikande_ikanniyotteha', expression=IKANDE_EXPRESSION,
                   reading='いかんで / いかんによっては', meaning='depending on',
                   match=match_ikande)
RULES = (RULE,)
