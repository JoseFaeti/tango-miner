"""A conclusion limited to the scope of the preceding evidence."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_kagirideha(tokens):
    for i in range(1, len(tokens) - 2):
        anchor, kagiri, de, ha = tokens[i - 1:i + 3]
        pos = anchor['pos']
        predicate = (pos[:1] in (['動詞'], ['助動詞'], ['形容詞'])
                     and len(pos) >= 6 and pos[5].startswith(('連体形', '終止形')))
        nominal = (anchor['surface'] == 'の' and pos[:1] == ['助詞']
                   and i >= 2 and tokens[i - 2]['pos'][:1] in (['名詞'], ['代名詞'])
                   and adjacent(tokens[i - 2], anchor))
        if not (predicate or nominal):
            continue
        if not (kagiri['surface'] in ('限り', 'かぎり')
                and kagiri['lemma'] in ('限り', '限る', 'かぎり', 'かぎる')
                and kagiri['pos'][:1] in (['名詞'], ['動詞'])
                and de['surface'] == 'で' and de['pos'][:1] == ['助詞']
                and ha['surface'] == 'は' and ha['pos'][:1] == ['助詞']
                and adjacent(anchor, kagiri) and adjacent(kagiri, de) and adjacent(de, ha)):
            continue
        # A copular negative such as 嬉しい限りではない is a different construction.
        if (i + 3 < len(tokens) and tokens[i + 3]['lemma'] in ('ない', '有る', 'ある')
                and adjacent(ha, tokens[i + 3])):
            continue
        yield kagiri['span'][0], ha['span'][1]


RULE = GrammarRule(order=3, id='kagirideha', expression='〜限りでは',
                   reading='かぎりでは', meaning='as far as; to the extent that',
                   match=match_kagirideha)
RULES = (RULE,)
