"""Source-aligned case and conditional conjunctive particle と."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_to(tokens):
    for i in range(1, len(tokens)):
        previous, particle = tokens[i - 1:i + 1]
        if (particle['surface'] != 'と' or particle['lemma'] != 'と'
                or particle['pos'][:2] not in (['助詞', '格助詞'], ['助詞', '接続助詞'])
                or previous['pos'][:1] == ['空白']
                or not adjacent(previous, particle)):
            continue
        if particle['pos'][1] == '接続助詞':
            pos = previous['pos']
            if (pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                    or len(pos) < 6 or not pos[5].startswith(('終止形', '連体形'))
                    or not any(t['pos'][:1] not in (['補助記号'], ['空白'])
                               for t in tokens[i + 1:])):
                continue
        if previous['pos'][:1] == ['補助記号'] and previous['pos'][1:2] != ['括弧閉']:
            continue
        yield tuple(particle['span'])


RULE = GrammarRule(order=53, id='to', expression='〜と', reading='と',
                   meaning='quotation; with; and; if; when', match=match_to)

RULES = (RULE,)
