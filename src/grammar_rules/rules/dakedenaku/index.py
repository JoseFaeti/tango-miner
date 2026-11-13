"""Additive だけでなく and its topic-marked and colloquial forms."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

DAKEDENAKU_EXPRESSION = '〜だけでなく'


def match_dakedenaku(tokens):
    for i in range(1, len(tokens) - 2):
        previous, dake, de = tokens[i - 1:i + 2]
        if (dake['surface'] != 'だけ' or dake['lemma'] != 'だけ'
                or dake['pos'][:2] != ['助詞', '副助詞']
                or previous['pos'][:1] == ['空白']
                or (previous['pos'][:1] == ['補助記号']
                    and previous['pos'][1:2] != ['括弧閉'])
                or not adjacent(previous, dake)):
            continue
        if not ((de['surface'] in ('で', 'じゃ') and de['lemma'] == 'だ'
                 and de['pos'][:1] == ['助動詞'])
                or (de['surface'] == 'で' and de['lemma'] == 'で'
                    and de['pos'][:1] == ['助詞'])):
            continue
        end = i + 2
        if end < len(tokens) and tokens[end]['surface'] == 'は' and tokens[end]['pos'][:1] == ['助詞']:
            end += 1
        if end >= len(tokens):
            continue
        naku = tokens[end]
        parts = tokens[i:end + 1]
        if (naku['surface'] == 'なく' and naku['lemma'] == 'ない'
                and naku['pos'][:1] in (['形容詞'], ['助動詞'])
                and all(adjacent(a, b) for a, b in zip(parts, parts[1:]))):
            yield dake['span'][0], naku['span'][1]


RULE = GrammarRule(order=1, id='dakedenaku', expression=DAKEDENAKU_EXPRESSION,
                   reading='だけでなく', meaning='not only; as well as', match=match_dakedenaku)
RULES = (RULE,)
