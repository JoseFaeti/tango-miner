"""Concessive qualification with とは言っても."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

TOHAITSUTEMO_EXPRESSION = '〜とはいっても'


def match_tohaitsutemo(tokens):
    for i in range(len(tokens) - 4):
        particle, wa, verb, te, mo = tokens[i:i + 5]
        if (particle['surface'] != 'と' or particle['pos'][:2] != ['助詞', '格助詞']
                or wa['surface'] != 'は' or wa['pos'][:2] != ['助詞', '係助詞']
                or verb['surface'] not in ('いっ', '言っ')
                or verb['lemma'] not in ('いう', '言う') or verb['pos'][:1] != ['動詞']
                or te['surface'] != 'て' or te['pos'][:2] != ['助詞', '接続助詞']
                or mo['surface'] != 'も' or mo['pos'][:2] != ['助詞', '係助詞']
                or not all(adjacent(a, b) for a, b in zip((particle, wa, verb, te), (wa, verb, te, mo)))):
            continue
        for following in tokens[i + 5:]:
            if following['surface'] in ('。', '！', '？', '!', '?'):
                break
            if following['pos'][:1] not in (['補助記号'], ['空白']):
                yield particle['span'][0], mo['span'][1]
                break


RULE = GrammarRule(order=2, id='tohaitsutemo', expression=TOHAITSUTEMO_EXPRESSION,
                   reading='とはいっても', meaning='although one says; even so',
                   match=match_tohaitsutemo)
RULES = (RULE,)
