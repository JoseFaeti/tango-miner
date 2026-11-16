"""Literary negative copula にあらず."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches

NIARAZU_EXPRESSION = '〜にあらず'


def match_niarazu(tokens):
    for i in range(1, len(tokens) - 2):
        previous, ni, stem, negative = tokens[i - 1:i + 3]
        if (previous['pos'][:1] in (['補助記号'], ['空白'])
                or not all(adjacent(a, b) for a, b in zip(
                    (previous, ni, stem), (ni, stem, negative)))
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']):
            continue
        if (stem['surface'] == '非' and stem['pos'][:1] == ['接頭辞']
                and negative['surface'] == 'ず' and negative['lemma'] == 'ず'):
            yield ni['span'][0], negative['span'][1]
            continue
        if (stem['surface'] not in ('あら', '有ら', '在ら')
                or stem['lemma'] not in ('ある', '有る', '在る')
                or stem['pos'][:1] != ['動詞'] or negative['lemma'] != 'ず'
                or negative['pos'][:1] != ['助動詞']):
            continue
        if negative['surface'] in ('ず', 'ざる'):
            yield ni['span'][0], negative['span'][1]
        elif (negative['surface'] == 'ざれ'
              and next_matches(tokens, i + 2, 'ば', '助詞', 'ば')):
            yield ni['span'][0], tokens[i + 3]['span'][1]


RULE = GrammarRule(id='niarazu', expression=NIARAZU_EXPRESSION, reading='にあらず',
                   meaning='is not; is not the case (literary)', match=match_niarazu, order=36)
RULES = (RULE,)
