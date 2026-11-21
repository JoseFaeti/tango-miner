"""Introduce the existence of something by name."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, positive_verb_end


def match_toiumonogaaru(tokens):
    for i in range(1, len(tokens) - 4):
        previous, particle, verb, mono, ga, aru = tokens[i - 1:i + 5]
        if (particle['surface'] not in ('と', 'って') or particle['pos'][:1] != ['助詞']
                or verb['surface'] not in ('いう', '言う') or verb['lemma'] not in ('いう', '言う')
                or verb['pos'][:1] != ['動詞'] or mono['surface'] != 'もの'
                or mono['lemma'] != 'もの' or mono['pos'][:1] != ['名詞']
                or ga['surface'] != 'が' or ga['pos'][:1] != ['助詞']
                or previous['pos'][:1] == ['補助記号']
                or not all(adjacent(a, b) for a, b in zip(
                    (previous, particle, verb, mono, ga), (particle, verb, mono, ga, aru)))):
            continue
        end = positive_verb_end(tokens, i + 4, lemma='ある', base_forms={'ある'},
                                past_stems={'あっ'}, polite_stems={'あり'})
        if end is not None:
            yield particle['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=71, id='toiumonogaaru', expression='〜というものがある',
                   reading='というものがある', meaning='there is something called',
                   match=match_toiumonogaaru)
RULES = (RULE,)
