"""Selected predicate-attached concessive te mo constructions."""
from src.GrammarConstants import TE_MO_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches
from src.grammar_rules.rules.te_mo_ii import match_te_mo_ii


def match_te_mo(tokens):
    permission_starts = {start for start, _ in match_te_mo_ii(tokens)}
    for i in range(1, len(tokens) - 2):
        anchor, connective = tokens[i - 1:i + 1]
        pos = anchor['pos']
        if (pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(pos) < 6 or not pos[5].startswith('連用形')
                or not adjacent(anchor, connective)
                or connective['surface'] not in ('て', 'で')
                or connective['pos'][:2] != ['助詞', '接続助詞']
                or not next_matches(tokens, i, 'も', '助詞', 'も')
                or connective['span'][0] in permission_starts):
            continue
        following = tokens[i + 2:]
        if not following or following[0]['surface'] in ('。', '！', '？', '!', '?'):
            continue
        if not any(t['pos'][:1] != ['補助記号'] for t in following):
            continue
        yield connective['span'][0], tokens[i + 1]['span'][1]


RULE = GrammarRule(
    id='te_mo', expression=TE_MO_GRAMMAR_EXPRESSION, reading='ても',
    meaning='even if; even though', match=match_te_mo,
)
