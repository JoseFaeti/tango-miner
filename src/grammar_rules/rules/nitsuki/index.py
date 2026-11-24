"""Formal noun-attached につき: cause, distribution, or topic."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nitsuki(tokens):
    for i in range(1, len(tokens) - 1):
        before, ni, tsuki = tokens[i - 1:i + 2]
        if (before['pos'][:1] not in (['名詞'], ['代名詞'])
                and before['pos'][:2] != ['接尾辞', '名詞的']):
            continue
        if (ni['surface'] != 'に' or ni['pos'][:2] != ['助詞', '格助詞']
                or (tsuki['surface'], tsuki['lemma']) not in (
                    ('つき', 'つく'), ('付き', '付く'))
                or tsuki['pos'][:1] != ['動詞']
                or not adjacent(before, ni) or not adjacent(ni, tsuki)):
            continue
        following = tokens[i + 2] if i + 2 < len(tokens) else None
        if (following and adjacent(tsuki, following)
                and (following['pos'][:1] in (['助動詞'], ['動詞'])
                     or following['surface'] in ('て', 'つつ', 'ながら'))):
            continue
        yield ni['span'][0], tsuki['span'][1]


RULE = GrammarRule(id='nitsuki', expression='〜につき', reading='につき',
                   meaning='because of; per; concerning (formal)',
                   match=match_nitsuki, order=11)
RULES = (RULE,)
