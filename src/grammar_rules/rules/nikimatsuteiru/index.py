"""Strong conviction expressed by に決まっている."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.te_iru.index import match_te_iru


def match_nikimatsuteiru(tokens):
    continuations = dict(match_te_iru(tokens))
    for i in range(1, len(tokens) - 3):
        previous, ni, kimat, te = tokens[i - 1:i + 3]
        if not all(adjacent(a, b) for a, b in ((previous, ni), (ni, kimat), (kimat, te))):
            continue
        pos = previous['pos']
        nominal = pos[:1] in (['名詞'], ['代名詞'], ['形状詞'])
        predicate = (pos[:1] in (['動詞'], ['形容詞'], ['助動詞']) and len(pos) >= 6
                     and pos[5].startswith(('終止形', '連体形')))
        if not (nominal or predicate):
            continue
        if ni['surface'] != 'に' or (ni['lemma'], ni['pos'][0]) not in (('に', '助詞'), ('だ', '助動詞')):
            continue
        if (kimat['surface'] not in ('決まっ', 'きまっ') or kimat['lemma'] not in ('決まる', 'きまる')
                or kimat['pos'][:1] != ['動詞'] or len(kimat['pos']) < 6
                or not kimat['pos'][5].startswith('連用形')):
            continue
        if te['surface'] != 'て' or te['pos'][:2] != ['助詞', '接続助詞']:
            continue
        end = continuations.get(te['span'][0])
        if end is not None:
            yield ni['span'][0], end


RULE = GrammarRule(order=36, id='nikimatsuteiru', expression='〜に決まっている',
                   reading='にきまっている', meaning='surely; must be', match=match_nikimatsuteiru)
RULES = (RULE,)
