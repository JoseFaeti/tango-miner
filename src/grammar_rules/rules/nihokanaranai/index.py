"""Identity or cause emphasized by にほかならない."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_suffix_end, next_matches, polite_suffix_end

NIHOKANARANAI_EXPRESSION = '〜にほかならない'


def match_nihokanaranai(tokens):
    for i in range(1, len(tokens) - 3):
        previous, ni, hoka, nari, suffix = tokens[i - 1:i + 4]
        if (previous['pos'][:1] in (['補助記号'], ['空白'])
                or not all(adjacent(a, b) for a, b in zip(
                    (previous, ni, hoka, nari), (ni, hoka, nari, suffix)))
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or hoka['surface'] not in ('ほか', '他')
                or hoka['lemma'] != hoka['surface'] or hoka['pos'][:1] != ['名詞']
                or nari['lemma'] != 'なる' or nari['pos'][:1] != ['動詞']):
            continue
        end = None
        if nari['surface'] == 'なら':
            end = negative_suffix_end(tokens, i + 3, pos='助動詞', conditional=True)
            if (suffix['surface'] == 'なく' and suffix['lemma'] == 'ない'
                    and next_matches(tokens, i + 3, 'て', '助詞', 'て')):
                end = i + 4
        elif nari['surface'] == 'なり' and suffix['lemma'] == 'ます':
            end = polite_suffix_end(tokens, i + 3, forms=('negative',))
        if end is not None:
            yield ni['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=36, id='nihokanaranai', expression=NIHOKANARANAI_EXPRESSION,
                   reading='にほかならない', meaning='nothing other than; precisely',
                   match=match_nihokanaranai)
RULES = (RULE,)
