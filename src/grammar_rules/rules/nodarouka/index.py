"""Explanatory questions expressing uncertainty or wondering."""
NODAROUKA_EXPRESSION = '〜のだろうか'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nodarouka(tokens):
    for i in range(1, len(tokens) - 2):
        predicate, marker, copula, ka = tokens[i - 1:i + 3]
        if (predicate['pos'][:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(predicate['pos']) < 6
                or not predicate['pos'][5].startswith(('連体形', '終止形'))
                or marker['surface'] not in ('の', 'ん')
                or marker['lemma'] != marker['surface']
                or marker['pos'][:2] != ['助詞', '準体助詞']
                or (copula['lemma'], copula['surface']) not in
                   (('だ', 'だろう'), ('です', 'でしょう'))
                or copula['pos'][:1] != ['助動詞']
                or ka['surface'] != 'か' or ka['lemma'] != 'か'
                or ka['pos'][:2] not in (['助詞', '終助詞'], ['助詞', '副助詞'])
                or not all(adjacent(a, b) for a, b in
                           ((predicate, marker), (marker, copula), (copula, ka)))):
            continue
        yield marker['span'][0], ka['span'][1]


RULE = GrammarRule(order=59, id='nodarouka', expression=NODAROUKA_EXPRESSION,
                   reading='のだろうか', meaning='I wonder whether; why might it be',
                   match=match_nodarouka)

RULES = (RULE,)
