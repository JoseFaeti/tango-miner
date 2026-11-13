"""Verb stem + nominal かけ with a finite copula."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def match_kakeda(tokens):
    for i in range(len(tokens) - 1):
        noun, copula = tokens[i:i + 2]
        surface = noun['surface']
        if not (noun['pos'][:1] == ['名詞'] and len(surface) > 2
                and surface.endswith(('かけ', '掛け'))
                and '\u3041' <= surface[-3] <= '\u3096'
                and adjacent(noun, copula) and copula['pos'][:1] == ['助動詞']):
            continue
        if (copula['lemma'], copula['surface']) in (('だ', 'だ'), ('です', 'です')):
            end = i + 1
        elif ((copula['lemma'], copula['surface']) in (('だ', 'だっ'), ('です', 'でし'))
              and next_matches(tokens, i + 1, 'た', '助動詞', 'た')):
            end = i + 2
        else:
            continue
        yield noun['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='kakeda', expression='〜かけだ', reading='かけだ',
                   meaning='started but unfinished; in progress', match=match_kakeda)
RULES = (RULE,)
