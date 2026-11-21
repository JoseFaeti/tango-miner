"""Concessive というものの with an explicit clause boundary."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_toiumonono(tokens):
    for i in range(1, len(tokens) - 5):
        previous, to, verb, mono, no, comma = tokens[i - 1:i + 5]
        if (previous['pos'][:1] in (['補助記号'], ['空白'])
                or to['surface'] != 'と' or to['pos'][:2] != ['助詞', '格助詞']
                or verb['surface'] not in ('いう', '言う')
                or verb['lemma'] not in ('いう', '言う') or verb['pos'][:1] != ['動詞']
                or mono['surface'] != 'もの' or mono['pos'][:1] != ['名詞']
                or no['surface'] != 'の' or no['pos'][:1] != ['助詞']
                or comma['surface'] not in ('、', '，', ',')
                or not all(adjacent(a, b) for a, b in zip(
                    (previous, to, verb, mono, no), (to, verb, mono, no, comma)))):
            continue
        for following in tokens[i + 5:]:
            if following['surface'] in ('。', '！', '？', '!', '?'):
                break
            if following['pos'][:1] not in (['補助記号'], ['空白']):
                yield to['span'][0], no['span'][1]
                break


RULE = GrammarRule(order=2, id='toiumonono', expression='〜というものの',
                   reading='というものの', meaning='although; even though',
                   match=match_toiumonono)
RULES = (RULE,)
