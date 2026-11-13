"""Formal noun + において / における."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NIOITE_NIOKERU_EXPRESSION = '〜において / 〜における'


def match_nioite_niokeru(tokens):
    for i in range(1, len(tokens) - 1):
        noun, ni, verb = tokens[i - 1:i + 2]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['pos'][:1] != ['動詞']
                or not adjacent(noun, ni) or not adjacent(ni, verb)):
            continue
        if verb['surface'] == 'おける' and verb['lemma'] == 'おける':
            yield ni['span'][0], verb['span'][1]
        elif (verb['surface'] == 'おけ' and verb['lemma'] == 'おく'
              and i + 2 < len(tokens)):
            ru = tokens[i + 2]
            if ru['surface'] == 'る' and ru['lemma'] == 'り' and ru['pos'][:1] == ['助動詞'] and adjacent(verb, ru):
                yield ni['span'][0], ru['span'][1]
        elif (verb['surface'] == 'おい' and verb['lemma'] == 'おく'
              and i + 2 < len(tokens)):
            te = tokens[i + 2]
            if te['surface'] == 'て' and te['pos'][:1] == ['助詞'] and adjacent(verb, te):
                yield ni['span'][0], te['span'][1]


RULE = GrammarRule(order=7, id='nioite_niokeru', expression=NIOITE_NIOKERU_EXPRESSION,
                   reading='において / における', meaning='in; at; concerning',
                   match=match_nioite_niokeru)
RULES = (RULE,)
