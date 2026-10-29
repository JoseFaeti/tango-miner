"""Source-aligned verbal negative auxiliary."""
from src.GrammarConstants import NAI_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


RULE = GrammarRule(
    id='verbal_nai', expression=NAI_GRAMMAR_EXPRESSION, reading='ない',
    meaning=('Grammar: the negative auxiliary ない attaches to a verb in its '
             'irrealis form to negate the action or state.'),
    match=lambda tokens: match_nai(tokens),
)


def match_nai(tokens):
    for i in range(1, len(tokens)):
        previous, negative = tokens[i - 1:i + 1]
        if (not adjacent(previous, negative) or negative['lemma'] != 'ない'
                or negative['pos'][:1] != ['助動詞']
                or len(previous['pos']) < 6
                or not previous['pos'][5].startswith('未然形')):
            continue
        if not (previous['pos'][:1] == ['動詞'] or
                (previous['pos'][:1] == ['助動詞'] and
                 previous['lemma'] in ('れる', 'られる', 'せる', 'させる'))):
            continue
        end = i
        if negative['surface'] == 'なかっ':
            if not next_matches(tokens, end, 'た', '助動詞', 'た'):
                continue
            end += 1
        elif negative['surface'] == 'なけれ':
            if not next_matches(tokens, end, 'ば', '助詞', 'ば'):
                continue
            end += 1
        elif negative['surface'] not in ('ない', 'なく'):
            continue
        yield negative['span'][0], tokens[end]['span'][1]
