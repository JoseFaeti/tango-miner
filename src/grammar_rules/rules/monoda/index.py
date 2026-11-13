"""Predicate followed by finite ものだ, with inherent nominal ambiguity."""
MONODA_EXPRESSION = '〜ものだ'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def match_monoda(tokens):
    for i in range(1, len(tokens) - 1):
        previous, mono, copula = tokens[i - 1:i + 2]
        pos = previous['pos']
        if (pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(pos) < 6 or not pos[5].startswith(('連体形', '終止形'))
                or mono['surface'] != 'もの' or mono['lemma'] != 'もの'
                or mono['pos'][:1] != ['名詞']
                or not adjacent(previous, mono) or not adjacent(mono, copula)
                or copula['pos'][:1] != ['助動詞']):
            continue
        if (copula['lemma'], copula['surface']) in (('だ', 'だ'), ('です', 'です')):
            end = i + 1
        elif ((copula['lemma'], copula['surface']) in (('だ', 'だっ'), ('です', 'でし'))
              and next_matches(tokens, i + 1, 'た', '助動詞', 'た')):
            end = i + 2
        else:
            continue
        # ものだから is a separate causal construction.
        if next_matches(tokens, end, 'から', '助詞', 'から'):
            continue
        yield mono['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=72, id='monoda', expression=MONODA_EXPRESSION, reading='ものだ',
                   meaning='generally; used to; should; how (it is)', match=match_monoda)

RULES = (RULE,)
