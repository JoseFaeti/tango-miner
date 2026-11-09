"""Volitional verb + とする: attempt or be about to do something."""
from src.GrammarConstants import YOU_TO_SURU_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import next_matches, positive_verb_end, negative_predicate_end


def match_you_to_suru(tokens):
    for i in range(len(tokens)):
        verb = tokens[i]
        if not (verb['pos'][:1] == ['動詞'] and len(verb['pos']) >= 6
                and verb['pos'][5].startswith('意志推量形')
                and next_matches(tokens, i, 'と', '助詞', 'と')
                and next_matches(tokens, i + 1, 'する', '動詞')):
            continue
        j = i + 2
        end = positive_verb_end(tokens, j, lemma='する', base_forms={'する'},
                                past_stems={'し'}, polite_stems={'し'}, te_stems={'し'},
                                conditional_stems={'すれ'})
        if end is None:
            end = negative_predicate_end(tokens, j, plain_lemma='する', plain_stems={'し'},
                                         polite_lemma='する', polite_stems={'し'})
        if end is not None:
            yield tokens[i + 1]['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='you_to_suru', expression=YOU_TO_SURU_EXPRESSION,
                   reading='ようとする', meaning='try to do; be about to do',
                   match=match_you_to_suru)
