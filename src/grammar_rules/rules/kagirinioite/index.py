"""Limit a statement to the scope of a preceding condition or evidence."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.nioite_niokeru.index import match_nioite_niokeru


def match_kagirinioite(tokens):
    for start, end in match_nioite_niokeru(tokens):
        for i in range(1, len(tokens) - 1):
            anchor, kagiri, ni = tokens[i - 1:i + 2]
            if ni['span'][0] != start or tokens[i + 2]['surface'] != 'おい':
                continue
            pos = anchor['pos']
            predicate = (pos[:1] in (['動詞'], ['助動詞'], ['形容詞'])
                         and len(pos) >= 6 and pos[5].startswith(('連体形', '終止形')))
            nominal = (anchor['surface'] == 'の' and pos[:1] == ['助詞']
                       and i >= 2 and tokens[i - 2]['pos'][:1] in (['名詞'], ['代名詞'])
                       and adjacent(tokens[i - 2], anchor))
            if ((predicate or nominal) and kagiri['surface'] in ('限り', 'かぎり')
                    and kagiri['lemma'] in ('限り', '限る', 'かぎり', 'かぎる')
                    and kagiri['pos'][:1] in (['名詞'], ['動詞'])
                    and adjacent(anchor, kagiri)):
                yield kagiri['span'][0], end


RULE = GrammarRule(order=3, id='kagirinioite', expression='〜限りにおいて',
                   reading='かぎりにおいて', meaning='insofar as; to the extent that',
                   match=match_kagirinioite)
RULES = (RULE,)
