"""Literary んばかりに, expressing an appearance or extreme degree."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nbakarini(tokens):
    for i in range(1, len(tokens) - 1):
        head, marker, ni = tokens[i - 1:i + 2]
        if (marker['surface'] != 'ばかり' or marker['lemma'] != 'ばかり'
                or marker['pos'][:2] != ['助詞', '副助詞']
                or ni['surface'] != 'に' or ni['lemma'] != 'に'
                or ni['pos'][:1] != ['助詞']
                or not adjacent(head, marker) or not adjacent(marker, ni)):
            continue
        pos = head['pos']
        if (head['surface'] == 'ん' and head['lemma'] in ('む', 'ぬ')
                and pos[:1] == ['助動詞'] and i >= 2):
            verb = tokens[i - 2]
            vpos = verb['pos']
            if (adjacent(verb, head) and len(vpos) >= 6
                    and (vpos[0] == '動詞' or (vpos[0] == '助動詞'
                         and verb['lemma'] in ('れる', 'られる', 'せる', 'させる')))
                    and (vpos[5].startswith('未然形') or
                         (vpos[5].startswith('連用形') and
                          (vpos[4].startswith(('上一段', '下一段')) or verb['lemma'] == '来る')))):
                yield head['span'][0], ni['span'][1]
        elif (len(pos) >= 6 and pos[0] == '動詞' and head['surface'].endswith('ん')
              and pos[5].startswith('連体形-撥音便')):
            # Start inside the merged verb to preserve it as lexical evidence.
            yield head['span'][1] - 1, ni['span'][1]


RULE = GrammarRule(id='nbakarini', expression='〜んばかりに', reading='んばかりに',
                   meaning='as if about to; to the point that', match=match_nbakarini,
                   order=100)
RULES = (RULE,)
