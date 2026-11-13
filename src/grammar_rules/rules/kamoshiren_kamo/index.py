"""Colloquial possibility with かもしれん or sentence-final かも."""
KAMOSHIREN_KAMO_EXPRESSION = '〜かもしれん / 〜かも'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_kamoshiren_kamo(tokens):
    for i in range(1, len(tokens) - 1):
        previous, ka, mo = tokens[i - 1:i + 2]
        if (previous['pos'][:1] not in (['動詞'], ['形容詞'], ['形状詞'], ['助動詞'], ['名詞'], ['副詞'])
                or not adjacent(previous, ka) or not adjacent(ka, mo)
                or (ka['surface'], ka['lemma'], ka['pos'][:2]) != ('か', 'か', ['助詞', '副助詞'])
                or (mo['surface'], mo['lemma'], mo['pos'][:2]) != ('も', 'も', ['助詞', '係助詞'])):
            continue
        end = i + 1
        if end + 1 < len(tokens):
            following = tokens[end + 1]
            if following['surface'] in ('しれ', '知れ') and following['lemma'] in ('しれる', '知れる') and following['pos'][:1] == ['動詞']:
                if (not adjacent(mo, following) or end + 2 >= len(tokens) or not adjacent(following, tokens[end + 2])
                        or (tokens[end + 2]['surface'], tokens[end + 2]['lemma'], tokens[end + 2]['pos'][:1]) != ('ん', 'ぬ', ['助動詞'])):
                    continue
                end += 2
            elif following['pos'][:1] != ['補助記号'] and following['pos'][:2] != ['助詞', '終助詞']:
                continue
        yield ka['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=74, id='kamoshiren_kamo', expression=KAMOSHIREN_KAMO_EXPRESSION,
                   reading='かもしれん / かも', meaning='might; maybe (colloquial)',
                   match=match_kamoshiren_kamo)

RULES = (RULE,)
