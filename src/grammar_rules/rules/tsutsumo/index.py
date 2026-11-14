"""Concessive continuative verb + つつも."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

TSUTSUMO_EXPRESSION = '〜つつも'


def match_tsutsumo(tokens):
    for i in range(1, len(tokens) - 2):
        verb, particle, mo = tokens[i - 1:i + 2]
        if (verb['pos'][:1] not in (['動詞'], ['助動詞'])
                or len(verb['pos']) < 6 or not verb['pos'][5].startswith('連用形')
                or particle['surface'] != 'つつ' or particle['lemma'] != 'つつ'
                or particle['pos'][:2] != ['助詞', '接続助詞']
                or mo['surface'] != 'も' or mo['lemma'] != 'も'
                or mo['pos'][:2] != ['助詞', '係助詞']
                or not adjacent(verb, particle) or not adjacent(particle, mo)):
            continue
        for following in tokens[i + 2:]:
            if following['surface'] in ('。', '！', '？', '!', '?'):
                break
            if following['pos'][:1] != ['補助記号']:
                yield particle['span'][0], mo['span'][1]
                break


RULE = GrammarRule(order=128, id='tsutsumo', expression=TSUTSUMO_EXPRESSION,
                   reading='つつも', meaning='although; even though', match=match_tsutsumo)
RULES = (RULE,)
