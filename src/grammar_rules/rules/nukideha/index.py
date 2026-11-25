"""Noun + 抜きでは: without an essential item or condition."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def match_nukideha(tokens):
    for i in range(1, len(tokens) - 2):
        previous, nuki = tokens[i - 1], tokens[i]
        if (previous['pos'][:1] not in (['名詞'], ['代名詞'])
                or not adjacent(previous, nuki)
                or nuki['surface'] not in ('抜き', 'ぬき')
                or nuki['pos'][:1] != ['名詞']
                or not next_matches(tokens, i, 'で', '助詞', 'で')
                or not next_matches(tokens, i + 1, 'は', '助詞', 'は')):
            continue
        yield nuki['span'][0], tokens[i + 2]['span'][1]


RULE = GrammarRule(
    order=1, id='nukideha', expression='〜抜きでは',
    reading='ぬきでは', meaning='without; cannot without',
    match=match_nukideha,
)
RULES = (RULE,)
