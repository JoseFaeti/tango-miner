"""Recommendations with a past or negative verb followed by 方がいい."""
from src.GrammarConstants import TA_HOU_GA_II_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def match_ta_hou_ga_ii(tokens):
    for i in range(1, len(tokens) - 3):
        verb, suffix = tokens[i - 1:i + 1]
        if (verb['pos'][:1] not in (['動詞'], ['助動詞'])
                or len(verb['pos']) < 6 or not adjacent(verb, suffix)):
            continue
        if (verb['pos'][:1] == ['助動詞']
                and verb['lemma'] not in ('れる', 'られる', 'せる', 'させる')):
            continue
        past = (suffix['surface'] in ('た', 'だ') and suffix['lemma'] in ('た', 'だ')
                and suffix['pos'][:1] == ['助動詞'] and verb['pos'][5].startswith('連用形'))
        negative = (suffix['surface'] == 'ない' and suffix['lemma'] == 'ない'
                    and suffix['pos'][:1] == ['助動詞'] and verb['pos'][5].startswith('未然形'))
        if not (past or negative):
            continue
        hou = tokens[i + 1]
        if (not adjacent(suffix, hou) or hou['surface'] not in ('ほう', '方')
                or hou['pos'][:1] != ['名詞']
                or not next_matches(tokens, i + 1, 'が', '助詞', 'が')
                or not adjacent(tokens[i + 2], tokens[i + 3])):
            continue
        ii = tokens[i + 3]
        if ii['pos'][:1] != ['形容詞'] or ii['surface'] not in ('いい', 'よい', '良い'):
            continue
        yield suffix['span'][0], ii['span'][1]


RULE = GrammarRule(
    id='ta_hou_ga_ii', expression=TA_HOU_GA_II_EXPRESSION,
    reading='たほうがいい / ないほうがいい',
    meaning='had better do; should do; had better not do',
    match=match_ta_hou_ga_ii,
)
