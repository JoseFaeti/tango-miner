"""Noun + いかんを問わず: regardless of the circumstances."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.otowazu.index import match_otowazu

IKANOTOWAZU_EXPRESSION = '〜いかんを問わず'


def match_ikanotowazu(tokens):
    suffixes = dict(match_otowazu(tokens))
    for i in range(1, len(tokens) - 1):
        marker, following = tokens[i:i + 2]
        previous = tokens[i - 1]
        if previous['surface'] == 'の' and previous['pos'][:1] == ['助詞']:
            if i < 2 or not adjacent(tokens[i - 2], previous):
                continue
            previous = tokens[i - 2]
        if (previous['pos'][:1] not in (['名詞'], ['代名詞'])
                or not adjacent(tokens[i - 1], marker)
                or marker['surface'] not in ('いかん', '如何')
                or marker['lemma'] not in ('いかん', '如何')
                or marker['pos'][:1] != ['名詞']
                or not adjacent(marker, following)):
            continue
        end = suffixes.get(following['span'][0])
        if end is not None:
            yield marker['span'][0], end


RULE = GrammarRule(order=1, id='ikanotowazu', expression=IKANOTOWAZU_EXPRESSION,
                   reading='いかんをとわず', meaning='regardless of the outcome or circumstances',
                   match=match_ikanotowazu)
RULES = (RULE,)
