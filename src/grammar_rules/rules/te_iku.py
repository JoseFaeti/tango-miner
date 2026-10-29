"""Source-aligned verb て-form + いく construction."""
from src.GrammarConstants import TE_IKU_GRAMMAR_EXPRESSION
from src.GrammarShadow import polite_suffix_end
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


RULE = GrammarRule(
    id='te_iku', expression=TE_IKU_GRAMMAR_EXPRESSION, reading='ていく',
    meaning=('Grammar: verb て-form + いく can describe movement away or '
             'an action or change continuing onward. Context determines the meaning.'),
    match=lambda tokens: match_te_iku(tokens),
)


def match_te_iku(tokens):
    for i in range(1, len(tokens) - 1):
        verb, connective, iku = tokens[i - 1:i + 2]
        if (not adjacent(verb, connective) or not adjacent(connective, iku)
                or verb['pos'][:1] not in (['動詞'], ['助動詞'])
                or len(verb['pos']) < 6 or not verb['pos'][5].startswith('連用形')
                or connective['surface'] not in ('て', 'で')
                or connective['pos'][:2] != ['助詞', '接続助詞']
                or iku['lemma'] not in ('いく', '行く', 'ゆく')
                or iku['pos'][:2] != ['動詞', '非自立可能']):
            continue
        end = i + 1
        surface = iku['surface']
        if surface in ('いく', '行く', 'ゆく', 'いこう', '行こう', 'ゆこう'):
            pass
        elif surface in ('いっ', '行っ', 'ゆっ', 'いき', '行き', 'ゆき'):
            if surface in ('いっ', '行っ', 'ゆっ') and next_matches(tokens, end, 'た', '助動詞'):
                end += 1
            elif surface in ('いっ', '行っ', 'ゆっ') and next_matches(tokens, end, 'て', '助詞', 'て'):
                end += 1
            elif surface in ('いき', '行き', 'ゆき') and next_matches(tokens, end, 'ます', '助動詞'):
                end += 1
                suffix = tokens[end]
                end = polite_suffix_end(tokens, end)
                if suffix['surface'] in ('まし', 'ませ') and tokens[end] is suffix:
                    continue
            else:
                continue
        elif surface in ('いか', '行か', 'ゆか'):
            if not next_matches(tokens, end, 'ない', '助動詞'):
                continue
            end += 1
            if tokens[end]['surface'] == 'なかっ':
                if not next_matches(tokens, end, 'た', '助動詞'):
                    continue
                end += 1
        else:
            continue
        yield connective['span'][0], tokens[end]['span'][1]
