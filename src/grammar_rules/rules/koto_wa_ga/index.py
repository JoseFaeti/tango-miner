"""Repeated predicate + ことは + predicate + concessive connector."""
KOTO_WA_GA = '〜ことは〜が'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_koto_wa_ga(tokens):
    for i in range(1, len(tokens) - 3):
        koto, wa = tokens[i:i + 2]
        if (koto['surface'] != 'こと' or koto['pos'][:1] != ['名詞']
                or wa['surface'] != 'は' or wa['pos'][:1] != ['助詞']
                or not adjacent(tokens[i - 1], koto) or not adjacent(koto, wa)):
            continue
        left = i - 1
        if tokens[left]['pos'][:1] == ['助動詞']:
            left -= 1
        right = i + 2
        if left < 0 or tokens[left]['pos'][:1] not in (['動詞'], ['形容詞'], ['形状詞']):
            continue
        if left != i - 1 and not adjacent(tokens[left], tokens[i - 1]):
            continue
        if (tokens[left]['lemma'] != tokens[right]['lemma']
                or tokens[left]['pos'][:1] != tokens[right]['pos'][:1]
                or not adjacent(wa, tokens[right])):
            continue
        end = right + 1
        if left != i - 1:
            if (end >= len(tokens) or tokens[end]['pos'][:1] != ['助動詞']
                    or tokens[end]['lemma'] != tokens[i - 1]['lemma']
                    or not adjacent(tokens[right], tokens[end])):
                continue
            end += 1
        if (end >= len(tokens) or not adjacent(tokens[end - 1], tokens[end])
                or tokens[end]['surface'] not in ('が', 'けど', 'けれど', 'けれども')
                or tokens[end]['pos'][:2] != ['助詞', '接続助詞']):
            continue
        clause = []
        for token in tokens[end + 1:]:
            if token['surface'] in ('。', '！', '？', '!', '?'):
                break
            clause.append(token)
        if not any(t['pos'][:1] != ['補助記号'] for t in clause):
            continue
        yield koto['span'][0], wa['span'][1]


RULE = GrammarRule(order=66, id='koto_wa_ga', expression=KOTO_WA_GA, reading='ことはが',
                   meaning='although; admittedly ... but', match=match_koto_wa_ga)

RULES = (RULE,)
