"""A condition or extent expressed by a preceding predicate and 限り."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_kagiri(tokens):
    for i in range(1, len(tokens)):
        anchor, kagiri = tokens[i - 1:i + 1]
        pos = anchor['pos']
        predicate = (pos[:1] in (['動詞'], ['助動詞'], ['形容詞'])
                     and len(pos) >= 6 and pos[5].startswith(('連体形', '終止形'))
                     and anchor['lemma'] not in ('ない', '無い'))
        # Auxiliary endings need a verbal/adjectival host; な alone is not
        # evidence for the conditional (残念な限り is an extreme-degree use).
        host = i - 1
        while host >= 0 and tokens[host]['pos'][:1] == ['助動詞']:
            if host == 0 or not adjacent(tokens[host - 1], tokens[host]):
                break
            host -= 1
        predicate = predicate and tokens[host]['pos'][:1] in (['動詞'], ['形容詞'])
        if not predicate:
            continue
        if (kagiri['surface'] not in ('限り', 'かぎり')
                or kagiri['lemma'] not in ('限り', '限る', 'かぎり', 'かぎる')
                or kagiri['pos'][:1] not in (['名詞'], ['動詞'])
                or not adjacent(anchor, kagiri)):
            continue
        if i + 1 < len(tokens):
            following = tokens[i + 1]
            if (adjacent(kagiri, following)
                    and following['surface'] in ('で', 'だ', 'です', 'の', 'に')):
                continue
        yield kagiri['span']


RULE = GrammarRule(order=3, id='kagiri', expression='〜限り',
                   reading='かぎり', meaning='as long as; to the extent that',
                   match=match_kagiri)
RULES = (RULE,)
