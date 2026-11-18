"""Repeated verb followed by か〜ないかのうちに."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

KANAIKANOUCHINI_EXPRESSION = '〜か〜ないかのうちに'


def match_kanaikanouchini(tokens):
    for i in range(1, len(tokens) - 6):
        first = tokens[i - 1]
        if first['lemma'] == 'た' and first['pos'][:1] == ['助動詞'] and i >= 2:
            first = tokens[i - 2]
            if not adjacent(first, tokens[i - 1]):
                continue
        ka, verb, negative, ka2, no, uchi, ni = tokens[i:i + 7]
        if (first['pos'][:1] != ['動詞'] or verb['pos'][:1] != ['動詞']
                or first['lemma'] != verb['lemma']
                or len(verb['pos']) < 6 or not verb['pos'][5].startswith('未然形')
                or ka['surface'] != 'か' or ka['pos'][:1] != ['助詞']
                or negative['surface'] != 'ない' or negative['lemma'] != 'ない'
                or negative['pos'][:1] != ['助動詞']
                or ka2['surface'] != 'か' or ka2['pos'][:1] != ['助詞']
                or no['surface'] != 'の' or no['pos'][:2] != ['助詞', '格助詞']
                or uchi['surface'] not in ('うち', '内') or uchi['pos'][:1] != ['名詞']
                or ni['surface'] != 'に' or ni['pos'][:2] != ['助詞', '格助詞']
                or not all(adjacent(tokens[j], tokens[j + 1]) for j in range(i - 1, i + 6))):
            continue
        yield ka['span'][0], ni['span'][1]


RULE = GrammarRule(order=37, id='kanaikanouchini', expression=KANAIKANOUCHINI_EXPRESSION,
                   reading='かないかのうちに', meaning='almost as soon as; barely had ... when',
                   match=match_kanaikanouchini)
RULES = (RULE,)
