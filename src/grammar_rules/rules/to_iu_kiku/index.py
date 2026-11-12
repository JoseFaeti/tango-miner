"""Reported speech or information after a finite predicate."""
from src.GrammarConstants import TO_IU_KIKU_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, positive_verb_end, negative_predicate_end


def match_to_iu_kiku(tokens):
    forms = {
        '言う': ({'言う', 'いう'}, {'言っ', 'いっ'}, {'言い', 'いい'}, {'言わ', 'いわ'}),
        '聞く': ({'聞く', 'きく'}, {'聞い', 'きい'}, {'聞き', 'きき'}, {'聞か', 'きか'}),
    }
    for i in range(1, len(tokens) - 1):
        previous, particle, verb = tokens[i - 1:i + 2]
        pos = previous['pos']
        if (len(pos) < 6 or pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or not pos[5].startswith(('終止形', '連体形', '意志推量形'))
                or particle['surface'] != 'と' or particle['pos'][:1] != ['助詞']
                or verb['lemma'] not in forms or verb['pos'][:1] != ['動詞']
                or not adjacent(previous, particle) or not adjacent(particle, verb)):
            continue
        base, past, polite, negative = forms[verb['lemma']]
        end = positive_verb_end(tokens, i + 1, lemma=verb['lemma'], base_forms=base,
                                past_stems=past, polite_stems=polite)
        if end is None:
            end = negative_predicate_end(tokens, i + 1, plain_lemma=verb['lemma'],
                                         plain_stems=negative, polite_lemma=verb['lemma'],
                                         polite_stems=polite)
        if end is None and verb['surface'] in past and next_matches(tokens, i + 1, 'て', '助詞', 'て'):
            end = i + 2
        # Attributive という belongs to the separate naming/quotation rule.
        if (end is not None and verb['lemma'] == '言う' and end == i + 1
                and end + 1 < len(tokens) and tokens[end + 1]['pos'][:1] == ['名詞']
                and adjacent(tokens[end], tokens[end + 1])):
            continue
        if end is not None:
            yield particle['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='to_iu_kiku', expression=TO_IU_KIKU_EXPRESSION,
                   reading='という / ときく', meaning='say that; hear that',
                   match=match_to_iu_kiku)
