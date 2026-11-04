"""Source-aligned te-form + aru, indicating a resulting state."""
from src.GrammarConstants import TE_ARU_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, polite_suffix_end


def match_te_aru(tokens):
    for i in range(1, len(tokens) - 1):
        verb, connective, aru = tokens[i - 1:i + 2]
        if (not adjacent(verb, connective) or not adjacent(connective, aru)
                or verb['pos'][:1] not in (['動詞'], ['助動詞'])
                or len(verb['pos']) < 6 or not verb['pos'][5].startswith('連用形')
                or connective['surface'] not in ('て', 'で')
                or connective['pos'][:2] != ['助詞', '接続助詞']
                or aru['lemma'] not in ('ある', '有る')
                or aru['pos'][:2] != ['動詞', '非自立可能']):
            continue
        end = None
        if aru['surface'] in ('ある', '有る'):
            end = i + 1
        elif aru['surface'] in ('あっ', '有っ'):
            if (next_matches(tokens, i + 1, 'た', '助動詞')
                    or next_matches(tokens, i + 1, 'て', '助詞', 'て')):
                end = i + 2
        elif aru['surface'] in ('あり', '有り') and next_matches(tokens, i + 1, 'ます', '助動詞'):
            end = polite_suffix_end(tokens, i + 2)
        elif aru['surface'] in ('あれ', '有れ') and next_matches(tokens, i + 1, 'ば', '助詞', 'ば'):
            end = i + 2
        if end is not None:
            yield connective['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(
    id='te_aru', expression=TE_ARU_GRAMMAR_EXPRESSION, reading='てある',
    meaning='be in a state resulting from an action', match=match_te_aru,
)
