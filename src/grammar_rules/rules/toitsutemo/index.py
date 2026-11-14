"""Concessive qualification with と言っても."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

TOITSUTEMO_EXPRESSION = '〜といっても'


def match_toitsutemo(tokens):
    for i in range(len(tokens) - 3):
        particle, verb, te, mo = tokens[i:i + 4]
        if (particle['surface'] != 'と' or particle['pos'][:2] != ['助詞', '格助詞']
                or verb['surface'] not in ('いっ', '言っ')
                or verb['lemma'] not in ('いう', '言う') or verb['pos'][:1] != ['動詞']
                or te['surface'] != 'て' or te['pos'][:2] != ['助詞', '接続助詞']
                or mo['surface'] != 'も' or mo['pos'][:2] != ['助詞', '係助詞']
                or not all(adjacent(a, b) for a, b in zip((particle, verb, te), (verb, te, mo)))):
            continue
        for following in tokens[i + 4:]:
            if following['surface'] in ('。', '！', '？', '!', '?'):
                break
            if following['pos'][:1] not in (['補助記号'], ['空白']):
                yield particle['span'][0], mo['span'][1]
                break


RULE = GrammarRule(order=2, id='toitsutemo', expression=TOITSUTEMO_EXPRESSION,
                   reading='といっても', meaning='although one says; even so',
                   match=match_toitsutemo)
RULES = (RULE,)
