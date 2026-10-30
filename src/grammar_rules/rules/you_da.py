"""Source-aligned predicate or noun comparison followed by a finite よう copula."""
from src.GrammarConstants import YOU_DA_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def match_you_da(tokens):
    for i in range(1, len(tokens) - 1):
        previous, you, copula = tokens[i - 1:i + 2]
        if not adjacent(previous, you) or not adjacent(you, copula):
            continue
        if you['surface'] != 'よう' or you['lemma'] != 'よう' or you['pos'][:2] != ['形状詞', '助動詞語幹']:
            continue
        pos = previous['pos']
        predicate = (pos[:1] in (['動詞'], ['形容詞'], ['助動詞'])
                     and len(pos) >= 6 and pos[5].startswith(('終止形', '連体形')))
        noun = (previous['surface'] == 'の' and previous['lemma'] == 'の'
                and pos[:1] == ['助詞'] and i >= 2
                and adjacent(tokens[i - 2], previous)
                and tokens[i - 2]['pos'][:1] in (['名詞'], ['代名詞']))
        if not (predicate or noun) or copula['pos'][:1] != ['助動詞']:
            continue
        if (copula['lemma'], copula['surface']) in (('だ', 'だ'), ('です', 'です')):
            end = i + 1
        elif ((copula['lemma'], copula['surface']) in (('だ', 'だっ'), ('です', 'でし'))
              and next_matches(tokens, i + 1, 'た', '助動詞', 'た')):
            end = i + 2
        else:
            continue
        yield you['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='you_da', expression=YOU_DA_GRAMMAR_EXPRESSION,
                   reading='ようだ', meaning='seems; appears; is like', match=match_you_da)
