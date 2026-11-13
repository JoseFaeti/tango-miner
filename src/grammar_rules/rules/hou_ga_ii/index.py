"""Non-past verb recommendations with 方がいい."""
HOU_GA_II_EXPRESSION = '〜ほうがいい'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def match_hou_ga_ii(tokens):
    for i in range(1, len(tokens) - 2):
        verb, hou = tokens[i - 1:i + 1]
        if (verb['pos'][:1] not in (['動詞'], ['助動詞'])
                or len(verb['pos']) < 6 or not verb['pos'][5].startswith('連体形')
                or (verb['pos'][:1] == ['助動詞']
                    and verb['lemma'] not in ('れる', 'られる', 'せる', 'させる'))
                or not adjacent(verb, hou)
                or hou['surface'] not in ('ほう', '方') or hou['pos'][:1] != ['名詞']
                or not next_matches(tokens, i, 'が', '助詞', 'が')
                or not adjacent(tokens[i + 1], tokens[i + 2])):
            continue
        ii = tokens[i + 2]
        if ii['pos'][:1] == ['形容詞'] and ii['surface'] in ('いい', 'よい', '良い'):
            yield hou['span'][0], ii['span'][1]


RULE = GrammarRule(order=162, id='hou_ga_ii', expression=HOU_GA_II_EXPRESSION,
                   reading='ほうがいい', meaning='it is better to; should',
                   match=match_hou_ga_ii)

RULES = (RULE,)
