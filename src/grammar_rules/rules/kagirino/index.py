"""An amount or extent expressed as a prenominal limit."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_kagirino(tokens):
    for i in range(1, len(tokens) - 1):
        anchor, kagiri, no = tokens[i - 1:i + 2]
        pos = anchor['pos']
        predicate = (pos[:1] in (['動詞'], ['助動詞'], ['形容詞'])
                     and len(pos) >= 6 and pos[5].startswith(('連体形', '終止形'))
                     and anchor['lemma'] not in ('ない', '無い'))
        nominal = (anchor['surface'] == 'の' and pos[:1] == ['助詞']
                   and i >= 2 and tokens[i - 2]['pos'][:1] in (['名詞'], ['代名詞'])
                   and adjacent(tokens[i - 2], anchor))
        if not (predicate or nominal):
            continue
        if (kagiri['surface'] in ('限り', 'かぎり')
                and kagiri['lemma'] in ('限り', '限る', 'かぎり', 'かぎる')
                and kagiri['pos'][:1] in (['名詞'], ['動詞'])
                and no['surface'] == 'の' and no['pos'][:1] == ['助詞']
                and adjacent(anchor, kagiri) and adjacent(kagiri, no)):
            yield kagiri['span'][0], no['span'][1]


RULE = GrammarRule(order=3, id='kagirino', expression='〜限りの',
                   reading='かぎりの', meaning='as much as; to the limit of',
                   match=match_kagirino)
RULES = (RULE,)
