"""Verb-stem そうになる, expressing a near occurrence."""
from src.GrammarConstants import SOU_NI_NARU_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, positive_verb_end, negative_predicate_end


def match_sou_ni_naru(tokens):
    for i in range(1, len(tokens) - 2):
        verb, sou = tokens[i - 1:i + 1]
        pos = verb['pos']
        if (pos[:1] != ['動詞'] or len(pos) < 6 or not pos[5].startswith('連用形')
                or not adjacent(verb, sou) or sou['surface'] != 'そう'
                or sou['lemma'] != 'そう' or sou['pos'][:2] != ['形状詞', '助動詞語幹']
                or not next_matches(tokens, i, 'だ', '助動詞', 'に')
                or not next_matches(tokens, i + 1, 'なる', '動詞')):
            continue
        n = i + 2
        end = positive_verb_end(tokens, n, lemma='なる', base_forms={'なる'},
                                past_stems={'なっ'}, polite_stems={'なり'})
        if end is None:
            end = negative_predicate_end(tokens, n, plain_lemma='なる', plain_stems={'なら'},
                                         polite_lemma='なる', polite_stems={'なり'})
        if (end is None and tokens[n]['surface'] == 'なっ'
                and next_matches(tokens, n, 'て', '助詞', 'て')):
            end = n + 1
        if end is not None:
            yield sou['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='sou_ni_naru', expression=SOU_NI_NARU_EXPRESSION,
                   reading='そうになる', meaning='almost; nearly; be on the verge of',
                   match=match_sou_ni_naru)
