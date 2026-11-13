"""Colloquial reported or emphatic んだって and copular だって."""
NDATTE_DATTE_EXPRESSION = '〜んだって / 〜だって'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.nda.index import match_nda


def match_ndatte_datte(tokens):
    nda_spans = set(match_nda(tokens))
    for i in range(1, len(tokens) - 1):
        copula, particle = tokens[i:i + 2]
        if (copula['surface'] != 'だ' or copula['lemma'] != 'だ'
                or copula['pos'][:1] != ['助動詞']
                or particle['surface'] != 'って' or particle['lemma'] != 'って'
                or particle['pos'][:1] != ['助詞'] or not adjacent(copula, particle)):
            continue
        # Limit the bare copula form to utterance-final contexts; elsewhere
        # noun + だって commonly means "even noun" or introduces a quotation.
        if i + 2 < len(tokens) and tokens[i + 2]['surface'] not in ('。', '！', '？', '!', '?'):
            continue
        preceding = tokens[i - 1]
        if not adjacent(preceding, copula):
            continue
        if (preceding['span'][0], copula['span'][1]) in nda_spans:
            if preceding['surface'] == 'ん':
                yield preceding['span'][0], particle['span'][1]
        elif preceding['pos'][:1] in (['名詞'], ['形状詞']):
            yield copula['span'][0], particle['span'][1]


RULE = GrammarRule(order=63, id='ndatte_datte', expression=NDATTE_DATTE_EXPRESSION,
                   reading='んだって / だって', meaning='I hear that; (emphatic assertion)',
                   match=match_ndatte_datte)

RULES = (RULE,)
