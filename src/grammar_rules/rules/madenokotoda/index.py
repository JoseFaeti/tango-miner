"""A course of action is all that remains to be done."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_madenokotoda(tokens):
    for verb, made, no, koto, copula in zip(
            tokens, tokens[1:], tokens[2:], tokens[3:], tokens[4:]):
        if (verb['pos'][:1] == ['動詞'] and len(verb['pos']) >= 6
                and verb['pos'][5].startswith(('終止形', '連体形'))
                and made['surface'] == made['lemma'] == 'まで'
                and made['pos'][:2] == ['助詞', '副助詞']
                and no['surface'] == no['lemma'] == 'の'
                and no['pos'][:1] == ['助詞']
                and koto['surface'] in ('こと', '事')
                and koto['pos'][:2] == ['名詞', '普通名詞']
                and (copula['lemma'], copula['surface']) in (('だ', 'だ'), ('です', 'です'))
                and copula['pos'][:1] == ['助動詞']
                and all(adjacent(a, b) for a, b in
                        ((verb, made), (made, no), (no, koto), (koto, copula)))):
            yield made['span'][0], copula['span'][1]


RULE = GrammarRule(id='madenokotoda', expression='〜までのことだ', reading='までのことだ',
                   meaning='only have to; all that remains is to',
                   match=match_madenokotoda, order=107)
RULES = (RULE,)
