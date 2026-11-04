"""Verb-attached ところ with a finite copula."""
from src.GrammarConstants import TOKORODA_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def match_tokoroda(tokens):
    for i in range(1, len(tokens) - 1):
        previous, noun, copula = tokens[i - 1:i + 2]
        verb = (previous['pos'][:1] == ['動詞'] and len(previous['pos']) >= 6
                and previous['pos'][5].startswith('連体形'))
        past = (i >= 2 and previous['surface'] in ('た', 'だ')
                and previous['lemma'] in ('た', 'だ') and previous['pos'][:1] == ['助動詞']
                and tokens[i - 2]['pos'][:1] == ['動詞']
                and adjacent(tokens[i - 2], previous))
        if not (verb or past):
            continue
        if (noun['surface'] not in ('ところ', '所') or noun['lemma'] not in ('ところ', '所')
                or noun['pos'][:1] != ['名詞'] or not adjacent(previous, noun)
                or not adjacent(noun, copula) or copula['pos'][:1] != ['助動詞']):
            continue
        if (copula['lemma'], copula['surface']) in (('だ', 'だ'), ('です', 'です')):
            end = i + 1
        elif ((copula['lemma'], copula['surface']) in (('だ', 'だっ'), ('です', 'でし'))
              and next_matches(tokens, i + 1, 'た', '助動詞', 'た')):
            end = i + 2
        else:
            continue
        yield noun['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='tokoroda', expression=TOKORODA_EXPRESSION,
                   reading='ところだ', meaning='about to; in the middle of; just finished',
                   match=match_tokoroda)
