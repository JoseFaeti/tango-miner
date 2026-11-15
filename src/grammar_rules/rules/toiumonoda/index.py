"""Judgments expressed with というものだ."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

TOIUMONODA_EXPRESSION = '〜というものだ'


def match_toiumonoda(tokens):
    for i in range(1, len(tokens) - 3):
        previous, particle, verb, mono, copula = tokens[i - 1:i + 4]
        if (particle['surface'] not in ('と', 'って') or particle['pos'][:1] != ['助詞']
                or verb['surface'] not in ('いう', '言う') or verb['lemma'] not in ('いう', '言う')
                or verb['pos'][:1] != ['動詞'] or mono['surface'] != 'もの'
                or mono['lemma'] != 'もの' or mono['pos'][:1] != ['名詞']
                or copula['pos'][:1] != ['助動詞']
                or previous['pos'][:1] == ['補助記号']
                or not all(adjacent(a, b) for a, b in zip(
                    (previous, particle, verb, mono), (particle, verb, mono, copula)))):
            continue
        if (copula['lemma'], copula['surface']) in (('だ', 'だ'), ('です', 'です')):
            end = i + 3
        elif ((copula['lemma'], copula['surface']) in (('だ', 'だっ'), ('です', 'でし'))
              and i + 4 < len(tokens) and adjacent(copula, tokens[i + 4])
              and (tokens[i + 4]['lemma'], tokens[i + 4]['surface'], tokens[i + 4]['pos'][:1])
              == ('た', 'た', ['助動詞'])):
            end = i + 4
        else:
            continue
        if (end + 1 < len(tokens) and adjacent(tokens[end], tokens[end + 1])
                and tokens[end + 1]['surface'] == 'から' and tokens[end + 1]['pos'][:1] == ['助詞']):
            continue
        yield particle['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=71, id='toiumonoda', expression=TOIUMONODA_EXPRESSION,
                   reading='というものだ', meaning='that is what one calls; truly', match=match_toiumonoda)
RULES = (RULE,)
