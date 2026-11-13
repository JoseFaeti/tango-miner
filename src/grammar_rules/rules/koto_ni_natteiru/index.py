"""An established rule or arrangement expressed with ことになっている."""
from src.GrammarConstants import KOTO_NI_NATTEIRU_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import next_matches
from src.grammar_rules.rules.koto_ni_suru.index import has_verbal_predicate
from src.grammar_rules.rules.te_iru.index import match_te_iru


def match_koto_ni_natteiru(tokens):
    continuations = dict(match_te_iru(tokens))
    for i in range(1, len(tokens) - 3):
        koto = tokens[i]
        if (koto['surface'] != 'こと' or koto['lemma'] != 'こと'
                or koto['pos'][:1] != ['名詞'] or not has_verbal_predicate(tokens, i)
                or not next_matches(tokens, i, 'に', '助詞', 'に')
                or not next_matches(tokens, i + 1, 'なる', '動詞')):
            continue
        naru = tokens[i + 2]
        if (naru['surface'] != 'なっ' or len(naru['pos']) < 6
                or not naru['pos'][5].startswith('連用形')):
            continue
        end = continuations.get(tokens[i + 3]['span'][0])
        if end is not None:
            yield koto['span'][0], end


RULE = GrammarRule(
    id='koto_ni_natteiru', expression=KOTO_NI_NATTEIRU_EXPRESSION,
    reading='ことになっている', meaning='it is a rule or arrangement that',
    match=match_koto_ni_natteiru,
)
