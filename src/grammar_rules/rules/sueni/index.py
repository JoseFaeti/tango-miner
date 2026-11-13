"""Result after a process, expressed with 末に."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

SUENI_EXPRESSION = '〜末に'


def match_sueni(tokens):
    for i in range(2, len(tokens) - 1):
        before, previous, sue, particle = tokens[i - 2:i + 2]
        nominal = (before['pos'][:1] in (['名詞'], ['代名詞'])
                   and previous['surface'] == 'の' and previous['lemma'] == 'の'
                   and previous['pos'][:2] == ['助詞', '格助詞'])
        past_verb = (before['pos'][:1] == ['動詞']
                     and previous['surface'] in ('た', 'だ')
                     and previous['lemma'] in ('た', 'だ')
                     and previous['pos'][:1] == ['助動詞'])
        if not (nominal or past_verb):
            continue
        if (sue['surface'] not in ('末', 'すえ') or sue['lemma'] not in ('末', 'すえ')
                or sue['pos'][:1] != ['名詞'] or particle['surface'] != 'に'
                or particle['lemma'] != 'に' or particle['pos'][:2] != ['助詞', '格助詞']
                or not all(adjacent(a, b) for a, b in ((before, previous),
                                                       (previous, sue), (sue, particle)))):
            continue
        yield sue['span'][0], particle['span'][1]


RULE = GrammarRule(order=105, id='sueni', expression=SUENI_EXPRESSION,
                   reading='すえに', meaning='after (a process); in the end', match=match_sueni)
RULES = (RULE,)
