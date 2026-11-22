"""〜に限り: a formal restriction to a specified case."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nikagiri(tokens):
    for i in range(1, len(tokens) - 1):
        anchor, ni, kagiri = tokens[i - 1:i + 2]
        if (anchor['pos'][:1] not in (['名詞'], ['代名詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or kagiri['surface'] not in ('限り', 'かぎり')
                or kagiri['lemma'] not in ('限る', 'かぎる')
                or kagiri['pos'][:1] != ['動詞']
                or not adjacent(anchor, ni) or not adjacent(ni, kagiri)):
            continue
        if i + 2 < len(tokens):
            following = tokens[i + 2]
            if (following['surface'] == 'ます' and following['pos'][:1] == ['助動詞']
                    and adjacent(kagiri, following)):
                continue
        yield ni['span'][0], kagiri['span'][1]


RULE = GrammarRule(order=3, id='nikagiri', expression='〜に限り',
                   reading='にかぎり', meaning='only; limited to (formal)',
                   match=match_nikagiri)
RULES = (RULE,)
