"""Advice and exclamations ending in ことだ."""
KOTODA_EXPRESSION = '〜ことだ'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_kotoda(tokens):
    for i in range(1, len(tokens) - 1):
        previous, koto, copula = tokens[i - 1:i + 2]
        pos = previous['pos']
        if len(pos) < 6 or not pos[5].startswith('連体形'):
            continue
        verb = pos[:1] == ['動詞']
        adjective = pos[:1] == ['形容詞']
        negative = (previous['lemma'] == 'ない' and previous['surface'] == 'ない'
                    and pos[:1] == ['助動詞'] and i >= 2
                    and tokens[i - 2]['pos'][:1] == ['動詞']
                    and adjacent(tokens[i - 2], previous))
        na_adjective = (previous['surface'] == 'な' and previous['lemma'] == 'だ'
                        and pos[:1] == ['助動詞'] and i >= 2
                        and tokens[i - 2]['pos'][:1] == ['形状詞']
                        and adjacent(tokens[i - 2], previous))
        if not (verb or adjective or negative or na_adjective):
            continue
        if (koto['surface'] not in ('こと', '事') or koto['pos'][:2] != ['名詞', '普通名詞']
                or (copula['lemma'], copula['surface']) not in (('だ', 'だ'), ('です', 'です'))
                or copula['pos'][:1] != ['助動詞']
                or not adjacent(previous, koto) or not adjacent(koto, copula)):
            continue
        # Leave the longer reported-information construction to its own rule.
        if (verb and previous['lemma'] in ('言う', 'いう') and i >= 2
                and tokens[i - 2]['surface'] in ('と', 'って')
                and adjacent(tokens[i - 2], previous)):
            continue
        yield koto['span'][0], copula['span'][1]


RULE = GrammarRule(order=184, id='kotoda', expression=KOTODA_EXPRESSION,
                   reading='ことだ', meaning='should; how (exclamation)', match=match_kotoda)

RULES = (RULE,)
