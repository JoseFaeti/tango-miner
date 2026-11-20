"""Literary volitional form expressing an implied message."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_ntobakarini(tokens):
    for i in range(1, len(tokens) - 2):
        n, to, bakari, ni = tokens[i - 1:i + 3]
        if not (all(adjacent(a, b) for a, b in zip((n, to, bakari), (to, bakari, ni)))
                and to['surface'] == 'と' and to['lemma'] == 'と' and to['pos'][:1] == ['助詞']
                and bakari['surface'] == 'ばかり' and bakari['lemma'] == 'ばかり'
                and bakari['pos'][:2] == ['助詞', '副助詞']
                and ni['surface'] == 'に' and ni['lemma'] == 'に' and ni['pos'][:1] == ['助詞']):
            continue
        npos = n['pos']
        if (len(npos) >= 6 and npos[0] == '動詞' and n['surface'].endswith('ん')
                and npos[5].startswith('連体形-撥音便')):
            yield n['span'][1] - 1, ni['span'][1]
            continue
        if i < 2:
            continue
        head = tokens[i - 2]
        if not adjacent(head, n):
            continue
        pos = head['pos']
        verbal = (len(pos) >= 6 and (pos[0] == '動詞' or
                  (pos[0] == '助動詞' and head['lemma'] in ('れる', 'られる', 'せる', 'させる')))
                  and (pos[5].startswith('未然形') or
                       (pos[5].startswith('連用形') and
                        (pos[4].startswith(('上一段', '下一段')) or head['lemma'] == '来る'))))
        auxiliary = n['surface'] == 'ん' and n['lemma'] in ('ぬ', 'む') and n['pos'][:1] == ['助動詞']
        sahen = (n['surface'] == 'せん' and n['lemma'] == 'せん' and n['pos'][:1] == ['名詞']
                 and len(pos) >= 3 and pos[:1] == ['名詞'] and 'サ変可能' in pos)
        if verbal and auxiliary:
            yield n['span'][0], ni['span'][1]
        elif sahen:
            yield n['span'][1] - 1, ni['span'][1]


RULE = GrammarRule(id='ntobakarini', expression='〜んとばかりに', reading='んとばかりに',
                   meaning='as if to say; as though intending to', match=match_ntobakarini, order=100)
RULES = (RULE,)
