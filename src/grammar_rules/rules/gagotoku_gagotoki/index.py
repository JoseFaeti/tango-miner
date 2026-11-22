"""Literary comparisons with がごとく and がごとき."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_gagotoku_gagotoki(tokens):
    for i in range(1, len(tokens) - 1):
        previous, ga, gotoshi = tokens[i - 1:i + 2]
        pos = previous['pos']
        nominal = pos[:1] in (['名詞'], ['代名詞'])
        predicate = (pos[:1] in (['動詞'], ['形容詞'], ['助動詞'])
                     and len(pos) >= 6 and pos[5].startswith(('終止形', '連体形')))
        if not (nominal or predicate):
            continue
        if (ga['surface'] == ga['lemma'] == 'が' and ga['pos'][:2] == ['助詞', '格助詞']
                and (gotoshi['surface'], gotoshi['lemma']) in (
                    ('ごとく', 'ごとし'), ('如く', '如し'),
                    ('ごとき', 'ごとし'), ('如き', '如し'))
                and gotoshi['pos'][:1] == ['助動詞']
                and adjacent(previous, ga) and adjacent(ga, gotoshi)):
            yield ga['span'][0], gotoshi['span'][1]


RULE = GrammarRule(order=138, id='gagotoku_gagotoki', expression='〜がごとく / 〜がごとき',
                   reading='がごとく / がごとき', meaning='like; as if (literary)',
                   match=match_gagotoku_gagotoki)
RULES = (RULE,)
