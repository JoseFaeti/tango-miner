"""A habitual or ongoing decision expressed with ことにしている."""
KOTO_NI_SHITEIRU_EXPRESSION = '〜ことにしている'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import next_matches
from src.grammar_rules.rules.koto_ni_suru.index import has_verbal_predicate
from src.grammar_rules.rules.te_iru.index import match_te_iru


def match_koto_ni_shiteiru(tokens):
    continuations = dict(match_te_iru(tokens))
    for i in range(1, len(tokens) - 3):
        koto = tokens[i]
        if (koto['surface'] != 'こと' or koto['lemma'] != 'こと'
                or koto['pos'][:1] != ['名詞'] or not has_verbal_predicate(tokens, i)
                or not next_matches(tokens, i, 'に', '助詞', 'に')
                or not next_matches(tokens, i + 1, 'する', '動詞')):
            continue
        suru = tokens[i + 2]
        if (suru['surface'] != 'し' or len(suru['pos']) < 6
                or not suru['pos'][5].startswith('連用形')):
            continue
        continuation = tokens[i + 3]
        end = continuations.get(continuation['span'][0])
        if end is not None:
            yield koto['span'][0], end


RULE = GrammarRule(order=141,
    id='koto_ni_shiteiru', expression=KOTO_NI_SHITEIRU_EXPRESSION,
    reading='ことにしている', meaning='make it a habit or rule to do',
    match=match_koto_ni_shiteiru,
)

RULES = (RULE,)
