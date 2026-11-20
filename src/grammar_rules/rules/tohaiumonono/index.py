"""Concessive とはいうものの with an explicit clause boundary."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

TOHAIUMONONO_EXPRESSION = '〜とはいうものの'


def match_tohaiumonono(tokens):
    for i in range(len(tokens) - 6):
        to, wa, verb, mono, no, comma = tokens[i:i + 6]
        if (to['surface'] != 'と' or to['pos'][:2] != ['助詞', '格助詞']
                or wa['surface'] != 'は' or wa['pos'][:2] != ['助詞', '係助詞']
                or verb['surface'] not in ('いう', '言う')
                or verb['lemma'] not in ('いう', '言う') or verb['pos'][:1] != ['動詞']
                or mono['surface'] != 'もの' or mono['pos'][:1] != ['名詞']
                or no['surface'] != 'の' or no['pos'][:1] != ['助詞']
                or comma['surface'] not in ('、', '，', ',')
                or not all(adjacent(a, b) for a, b in zip(
                    (to, wa, verb, mono, no), (wa, verb, mono, no, comma)))):
            continue
        for following in tokens[i + 6:]:
            if following['surface'] in ('。', '！', '？', '!', '?'):
                break
            if following['pos'][:1] not in (['補助記号'], ['空白']):
                yield to['span'][0], no['span'][1]
                break


RULE = GrammarRule(order=2, id='tohaiumonono', expression=TOHAIUMONONO_EXPRESSION,
                   reading='とはいうものの', meaning='although; even though; nevertheless',
                   match=match_tohaiumonono)
RULES = (RULE,)
