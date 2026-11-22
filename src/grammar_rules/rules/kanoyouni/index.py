"""Predicate-attached かのように comparisons."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_kanoyouni(tokens):
    for i in range(1, len(tokens) - 3):
        previous, ka, no, you, ni = tokens[i - 1:i + 4]
        pos = previous['pos']
        if not (pos[:1] in (['動詞'], ['形容詞'], ['助動詞'])
                and len(pos) >= 6 and pos[5].startswith(('終止形', '連体形'))):
            continue
        if (ka['surface'] == ka['lemma'] == 'か' and ka['pos'][:1] == ['助詞']
                and no['surface'] == no['lemma'] == 'の' and no['pos'][:1] == ['助詞']
                and you['surface'] == you['lemma'] == 'よう'
                and you['pos'][:2] == ['形状詞', '助動詞語幹']
                and ni['surface'] == 'に' and ni['lemma'] == 'だ'
                and ni['pos'][:1] == ['助動詞']
                and all(adjacent(tokens[j], tokens[j + 1]) for j in range(i - 1, i + 3))):
            yield ka['span'][0], ni['span'][1]


RULE = GrammarRule(order=138, id='kanoyouni', expression='〜かのように',
                   reading='かのように', meaning='as if; as though', match=match_kanoyouni)
RULES = (RULE,)
