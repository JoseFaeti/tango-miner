"""Additive ばかりでなく and its topic-marked and colloquial forms."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

BAKARIDENAKU_EXPRESSION = '〜ばかりでなく'


def match_bakaridenaku(tokens):
    for i in range(1, len(tokens) - 2):
        previous, marker, de = tokens[i - 1:i + 2]
        if (marker['surface'] != 'ばかり' or marker['lemma'] != 'ばかり'
                or marker['pos'][:2] != ['助詞', '副助詞']
                or previous['pos'][:1] == ['空白']
                or (previous['pos'][:1] == ['補助記号']
                    and previous['pos'][1:2] != ['括弧閉'])
                or not adjacent(previous, marker)):
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
        if (end + 1 < len(tokens) and adjacent(naku, tokens[end + 1])
                and tokens[end + 1]['lemma'] == 'なる'
                and tokens[end + 1]['pos'][:1] == ['動詞']):
            continue
        if (naku['surface'] == 'なく' and naku['lemma'] == 'ない'
                and naku['pos'][:1] in (['形容詞'], ['助動詞'])
                and all(adjacent(a, b) for a, b in zip(parts, parts[1:]))):
            yield marker['span'][0], naku['span'][1]


RULE = GrammarRule(order=1, id='bakaridenaku', expression=BAKARIDENAKU_EXPRESSION,
                   reading='ばかりでなく', meaning='not only; as well as', match=match_bakaridenaku)
RULES = (RULE,)
