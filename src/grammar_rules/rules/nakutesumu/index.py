"""Negative なくて followed by 済む: avoid having to or get through without."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.nai.index import match_nai
from src.grammar_rules.rules.tesumu.index import sumu_end

NAKUTESUMU_EXPRESSION = '〜なくてすむ'


def match_nakutesumu(tokens):
    verbal_starts = {start for start, _ in match_nai(tokens)}
    for i in range(len(tokens) - 2):
        negative, connective, sumu = tokens[i:i + 3]
        if (negative['lemma'] != 'ない' or negative['surface'] not in ('なく', 'なくっ')
                or (negative['pos'][:1] == ['助動詞']
                    and negative['span'][0] not in verbal_starts)
                or negative['pos'][:1] not in (['助動詞'], ['形容詞'])
                or connective['surface'] != 'て'
                or connective['pos'][:2] != ['助詞', '接続助詞']
                or not adjacent(negative, connective)
                or not adjacent(connective, sumu)
                or sumu['lemma'] not in ('済む', 'すむ')
                or sumu['pos'][:1] != ['動詞']):
            continue
        end = sumu_end(tokens, i + 2)
        if end is not None:
            yield negative['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='nakutesumu', expression=NAKUTESUMU_EXPRESSION,
                   reading='なくてすむ', meaning='avoid having to; get through without',
                   match=match_nakutesumu, order=117)
RULES = (RULE,)
