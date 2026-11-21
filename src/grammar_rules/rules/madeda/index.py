"""A course of action is all that remains, or was all that was done."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_madeda(tokens):
    for i in range(1, len(tokens) - 1):
        previous, made, copula = tokens[i - 1:i + 2]
        if not (made['surface'] == made['lemma'] == 'まで'
                and made['pos'][:2] == ['助詞', '副助詞']
                and (copula['lemma'], copula['surface']) in (('だ', 'だ'), ('です', 'です'))
                and copula['pos'][:1] == ['助動詞']
                and adjacent(previous, made) and adjacent(made, copula)):
            continue
        nonpast = (previous['pos'][:1] == ['動詞'] and len(previous['pos']) >= 6
                   and previous['pos'][5].startswith(('終止形', '連体形')))
        past = (i >= 2 and previous['lemma'] == 'た'
                and previous['surface'] in ('た', 'だ')
                and previous['pos'][:1] == ['助動詞']
                and tokens[i - 2]['pos'][:1] == ['動詞']
                and adjacent(tokens[i - 2], previous))
        if nonpast or past:
            yield made['span'][0], copula['span'][1]


RULE = GrammarRule(id='madeda', expression='〜までだ', reading='までだ',
                   meaning='only; simply; all that remains is to',
                   match=match_madeda, order=107)
RULES = (RULE,)
