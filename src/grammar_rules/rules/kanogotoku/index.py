"""Predicate-attached literary かのごとく comparisons."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_kanogotoku(tokens):
    for i in range(1, len(tokens) - 2):
        previous, ka, no, gotoku = tokens[i - 1:i + 3]
        pos = previous['pos']
        if not (pos[:1] in (['動詞'], ['形容詞'], ['助動詞'])
                and len(pos) >= 6 and pos[5].startswith(('終止形', '連体形'))):
            continue
        if (ka['surface'] == ka['lemma'] == 'か' and ka['pos'][:1] == ['助詞']
                and no['surface'] == no['lemma'] == 'の' and no['pos'][:1] == ['助詞']
                and (gotoku['surface'], gotoku['lemma']) in (('ごとく', 'ごとし'), ('如く', '如し'))
                and gotoku['pos'][:1] == ['助動詞']
                and all(adjacent(tokens[j], tokens[j + 1]) for j in range(i - 1, i + 2))):
            yield ka['span'][0], gotoku['span'][1]


RULE = GrammarRule(order=138, id='kanogotoku', expression='〜かのごとく',
                   reading='かのごとく', meaning='as if; as though (literary)', match=match_kanogotoku)
RULES = (RULE,)
