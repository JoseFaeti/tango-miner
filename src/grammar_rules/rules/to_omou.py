"""Predicate quotation followed by an inflected form of 思う."""
from src.GrammarConstants import TO_OMOU_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, positive_verb_end, negative_suffix_end


def match_to_omou(tokens):
    for i in range(1, len(tokens) - 1):
        previous, particle, verb = tokens[i - 1:i + 2]
        if (not adjacent(previous, particle) or not adjacent(particle, verb)
                or previous['pos'][:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(previous['pos']) < 6
                or not previous['pos'][5].startswith(('終止形', '連体形', '意志推量形'))
                or particle['surface'] != 'と' or particle['pos'][:1] != ['助詞']
                or verb['lemma'] != '思う' or verb['pos'][:1] != ['動詞']):
            continue
        j = i + 1
        end = positive_verb_end(tokens, j, lemma='思う', base_forms={'思う'},
                                past_stems={'思っ'}, polite_stems={'思い'})
        if end is None and verb['surface'] == '思わ' and next_matches(tokens, j, 'ない', '助動詞'):
            end = negative_suffix_end(tokens, j + 1, pos='助動詞')
        if end is None and verb['surface'] == '思っ' and next_matches(tokens, j, 'て', '助詞', 'て'):
            end = j + 1
        if end is not None:
            yield particle['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(
    id='to_omou', expression=TO_OMOU_GRAMMAR_EXPRESSION, reading='とおもう',
    meaning='think that; believe that; (after a volitional form) intend to',
    match=match_to_omou,
)
