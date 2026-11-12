"""Topic or explanation introduced by というのは."""
from src.GrammarConstants import TOIUNOHA_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_toiunoha(tokens):
    for i in range(1, len(tokens) - 3):
        previous, particle, verb, nominalizer, topic = tokens[i - 1:i + 4]
        if (previous['pos'][:1] in (['補助記号'], ['空白'])
                or particle['surface'] not in ('と', 'って')
                or particle['pos'][:1] != ['助詞']
                or verb['surface'] not in ('いう', '言う')
                or verb['lemma'] not in ('いう', '言う') or verb['pos'][:1] != ['動詞']
                or nominalizer['surface'] != 'の' or nominalizer['pos'][:1] != ['助詞']
                or topic['surface'] != 'は' or topic['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in zip(tokens[i - 1:i + 3], tokens[i:i + 4]))):
            continue
        yield particle['span'][0], topic['span'][1]


RULE = GrammarRule(id='toiunoha', expression=TOIUNOHA_EXPRESSION,
                   reading='というのは', meaning='as for; what is meant by',
                   match=match_toiunoha)
