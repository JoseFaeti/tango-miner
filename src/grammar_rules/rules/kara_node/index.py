"""Reason clauses marked by から or ので."""
from src.GrammarConstants import KARA_NODE_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_kara_node(tokens):
    for i in range(1, len(tokens)):
        previous, token = tokens[i - 1:i + 1]
        if token['surface'] not in ('から', 'の'):
            continue
        pos = previous['pos']
        if (not adjacent(previous, token)
                or pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(pos) < 6):
            continue
        if (token['surface'] == 'から' and token['lemma'] == 'から'
                and token['pos'][:2] == ['助詞', '接続助詞']
                and pos[5].startswith('終止形')):
            yield tuple(token['span'])
        elif (token['surface'] == 'の' and token['lemma'] == 'の'
              and token['pos'][:2] == ['助詞', '準体助詞']
              and pos[5].startswith('連体形') and i + 1 < len(tokens)):
            de = tokens[i + 1]
            if (adjacent(token, de) and de['surface'] == 'で'
                    and de['lemma'] == 'だ' and de['pos'][:1] == ['助動詞']):
                yield token['span'][0], de['span'][1]


RULE = GrammarRule(id='reason_kara_node', expression=KARA_NODE_EXPRESSION,
                   reading='から / ので', meaning='because; since', match=match_kara_node)
