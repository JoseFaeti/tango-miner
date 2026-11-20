"""Literary volitional purpose: verb irrealis + んがために."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_ngatameni(tokens):
    for i in range(1, len(tokens) - 3):
        head, n, ga, tame, ni = tokens[i - 1:i + 4]
        if not (all(adjacent(a, b) for a, b in zip((head, n, ga, tame), (n, ga, tame, ni)))
                and ga['surface'] == 'が' and ga['lemma'] == 'が' and ga['pos'][:1] == ['助詞']
                and tame['surface'] in ('ため', '為') and tame['lemma'] in ('ため', '為')
                and tame['pos'][:1] == ['名詞'] and ni['surface'] == 'に'
                and ni['lemma'] == 'に' and ni['pos'][:2] == ['助詞', '格助詞']):
            continue
        pos = head['pos']
        verbal = (len(pos) >= 6 and (pos[0] == '動詞' or
                  (pos[0] == '助動詞' and head['lemma'] in ('れる', 'られる', 'せる', 'させる')))
                  and (pos[5].startswith('未然形') or
                       (pos[5].startswith('連用形') and
                        (pos[4].startswith(('上一段', '下一段')) or head['lemma'] == '来る'))))
        auxiliary = n['surface'] == 'ん' and n['lemma'] in ('ぬ', 'む') and n['pos'][:1] == ['助動詞']
        sahen = (n['surface'] == 'せん' and n['lemma'] == 'せん'
                 and n['pos'][:1] == ['名詞'] and len(pos) >= 3
                 and pos[:1] == ['名詞'] and 'サ変可能' in pos)
        if verbal and auxiliary:
            yield n['span'][0], ni['span'][1]
        elif sahen:
            yield n['span'][1] - 1, ni['span'][1]


RULE = GrammarRule(id='ngatameni', expression='〜んがために', reading='んがために',
                   meaning='in order to; for the purpose of (literary)',
                   match=match_ngatameni, order=100)
RULES = (RULE,)
