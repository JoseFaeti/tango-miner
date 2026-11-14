"""Continuative verb + つつ for simultaneous or concessive clauses."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.tsutsuaru.index import match_tsutsuaru
from src.grammar_rules.rules.tsutsumo.index import match_tsutsumo

TSUTSU_EXPRESSION = '〜つつ'


def match_tsutsu(tokens):
    progressive_starts = {start for start, _ in match_tsutsuaru(tokens)}
    concessive_starts = {start for start, _ in match_tsutsumo(tokens)}
    for i in range(1, len(tokens) - 1):
        verb, particle = tokens[i - 1:i + 1]
        if (verb['pos'][:1] not in (['動詞'], ['助動詞'])
                or len(verb['pos']) < 6 or not verb['pos'][5].startswith('連用形')
                or particle['surface'] != 'つつ' or particle['lemma'] != 'つつ'
                or particle['pos'][:2] != ['助詞', '接続助詞']
                or not adjacent(verb, particle)
                or particle['span'][0] in progressive_starts | concessive_starts):
            continue
        for following in tokens[i + 1:]:
            if following['surface'] in ('。', '！', '？', '!', '?'):
                break
            if following['pos'][:1] != ['補助記号']:
                yield tuple(particle['span'])
                break


RULE = GrammarRule(order=129, id='tsutsu', expression=TSUTSU_EXPRESSION,
                   reading='つつ', meaning='while doing; although', match=match_tsutsu)
RULES = (RULE,)
