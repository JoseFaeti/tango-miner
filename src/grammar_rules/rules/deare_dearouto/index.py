"""Concessive forms of the formal copula."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def match_deare_dearouto(tokens):
    for i in range(1, len(tokens) - 1):
        previous, de, aru = tokens[i - 1:i + 2]
        if (previous['pos'][:1] in (['空白'], ['補助記号'])
                or not adjacent(previous, de) or not adjacent(de, aru)):
            continue
        if (previous['surface'] in ('誰', 'だれ', '何', 'なに')
                and (de['surface'], de['lemma'], de['pos'][:1]) == ('で', 'で', ['助詞'])
                and (aru['surface'], aru['lemma'], aru['pos'][:1]) == ('あれ', 'あれ', ['感動詞'])):
            yield de['span'][0], aru['span'][1]
            continue
        if ((de['surface'], de['lemma'], de['pos'][:1]) != ('で', 'だ', ['助動詞'])
                or (aru['lemma'], aru['pos'][:1]) != ('ある', ['動詞'])):
            continue
        if aru['surface'] == 'あれ':
            if next_matches(tokens, i + 1, 'ば', '助詞', 'ば'):
                continue
            end = i + 1
        elif aru['surface'] == 'あろう' and next_matches(tokens, i + 1, 'と', '助詞', 'と'):
            end = i + 2
        else:
            continue
        yield de['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=1, id='deare_dearouto', expression='〜であれ / 〜であろうと',
                   reading='であれ / であろうと', meaning='even if; whoever or whatever it may be',
                   match=match_deare_dearouto)
RULES = (RULE,)
