"""An action or change in progress, expressed with つつある."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, positive_verb_end

TSUTSUARU_EXPRESSION = '〜つつある'


def match_tsutsuaru(tokens):
    for i in range(1, len(tokens) - 1):
        verb, particle, aru = tokens[i - 1:i + 2]
        if (verb['pos'][:1] not in (['動詞'], ['助動詞'])
                or len(verb['pos']) < 6 or not verb['pos'][5].startswith('連用形')
                or particle['surface'] != 'つつ' or particle['lemma'] != 'つつ'
                or particle['pos'][:2] != ['助詞', '接続助詞']
                or not adjacent(verb, particle) or not adjacent(particle, aru)):
            continue
        end = positive_verb_end(tokens, i + 1, lemma='ある', base_forms=('ある',),
                                past_stems=('あっ',), polite_stems=('あり',),
                                te_stems=('あっ',), conditional_stems=('あれ',))
        if end is not None:
            yield particle['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=128, id='tsutsuaru', expression=TSUTSUARU_EXPRESSION,
                   reading='つつある', meaning='be in the process of; be gradually changing',
                   match=match_tsutsuaru)
RULES = (RULE,)
