"""Rule for tai."""
from src.GrammarConstants import TAI_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, negative_suffix_end


RULE = GrammarRule(
    id='tai', expression=TAI_GRAMMAR_EXPRESSION, reading='たい',
    meaning=('Grammar: verb stem + たい expresses a desire to do the action. '
             'It conjugates like an adjective; context determines whose desire is meant.'),
    match=lambda tokens: match_tai(tokens),
)


def match_tai(tokens):
    for i in range(1, len(tokens)):
        verb, desire = tokens[i - 1:i + 1]
        if not (adjacent(verb, desire) and verb['pos'][:1] == ['動詞']
                and len(verb['pos']) >= 6 and verb['pos'][5].startswith('連用形')
                and desire['lemma'] == 'たい' and desire['pos'][:1] == ['助動詞']):
            continue
        end = i
        if desire['surface'] == 'たかっ':
            if not next_matches(tokens, end, 'た', '助動詞'):
                continue
            end += 1
        elif desire['surface'] == 'たく':
            if next_matches(tokens, end, 'ない', '形容詞'):
                end = negative_suffix_end(tokens, end + 1, pos='形容詞', continuative=True, extended=True)
                if end is None:
                    continue
            elif next_matches(tokens, end, 'て', '助詞', 'て'):
                end += 1
        elif desire['surface'] == 'たけれ':
            if not next_matches(tokens, end, 'ば', '助詞', 'ば'):
                continue
            end += 1
        elif desire['surface'] != 'たい':
            continue
        if next_matches(tokens, end, 'です', '助動詞', 'です'):
            end += 1
        yield desire['span'][0], tokens[end]['span'][1]
