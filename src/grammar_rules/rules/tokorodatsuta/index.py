"""Verb dictionary form followed by the past copula ところだった."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def match_tokorodatsuta(tokens):
    for i in range(1, len(tokens) - 2):
        verb, noun, copula = tokens[i - 1:i + 2]
        if not (verb['pos'][:1] == ['動詞'] and len(verb['pos']) >= 6
                and verb['pos'][5].startswith('連体形')
                and noun['surface'] in ('ところ', '所')
                and noun['lemma'] in ('ところ', '所')
                and noun['pos'][:1] == ['名詞']
                and adjacent(verb, noun) and adjacent(noun, copula)
                and copula['pos'][:1] == ['助動詞']
                and (copula['lemma'], copula['surface']) in (('だ', 'だっ'), ('です', 'でし'))
                and next_matches(tokens, i + 1, 'た', '助動詞', 'た')):
            continue
        # ているところだった describes an ongoing situation in the past.
        if (i >= 2 and tokens[i - 2]['surface'] in ('て', 'で')
                and tokens[i - 2]['pos'][:1] == ['助詞']
                and adjacent(tokens[i - 2], verb) and verb['lemma'] in ('いる', '居る')):
            continue
        yield noun['span'][0], tokens[i + 2]['span'][1]


RULE = GrammarRule(order=101, id='tokorodatsuta', expression='〜ところだった',
                   reading='ところだった', meaning='nearly; almost happened',
                   match=match_tokorodatsuta)
RULES = (RULE,)
