"""Rule for te_iru."""
from src.GrammarConstants import TE_IRU_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import has_next
from src.GrammarShadow import polite_suffix_end


RULE = GrammarRule(
    id='te_iru', expression=TE_IRU_GRAMMAR_EXPRESSION, reading='ている',
    meaning=('Grammar: verb て-form + いる. Can describe an ongoing action, '
             'a resulting state, or a habitual action, depending on context.'),
    match=lambda tokens: match_te_iru(tokens),
)


def match_te_iru(tokens):
    def suffix_end(i):
        if not has_next(tokens, i):
            return None
        suffix = tokens[i + 1]
        if suffix['pos'][:1] != ['助動詞']:
            return None
        if suffix['lemma'] == 'た':
            return i + 1
        if suffix['lemma'] == 'ます':
            end = polite_suffix_end(tokens, i + 1)
            if suffix['surface'] in ('まし', 'ませ') and end == i + 1:
                return None
            return end
        if suffix['lemma'] == 'ない':
            if suffix['surface'] == 'なかっ':
                if (not has_next(tokens, i + 1) or tokens[i + 2]['lemma'] != 'た'
                        or tokens[i + 2]['pos'][:1] != ['助動詞']):
                    return None
                return i + 2
            return i + 1
        return None

    for i in range(1, len(tokens)):
        previous, token = tokens[i - 1:i + 1]
        if (not has_next(tokens, i - 1) or previous['pos'][:1] not in (['動詞'], ['助動詞'])
                or len(previous['pos']) < 6 or not previous['pos'][5].startswith('連用形')):
            continue
        end = None
        if (token['surface'] == 'でない' and token['lemma'] == 'でない'
                and token['pos'][:1] == ['形容詞']):
            # Sudachi can merge the contracted connective and negative.
            end = i
        elif token['pos'][:1] == ['助動詞'] and token['lemma'] in ('てる', 'でる'):
            if token['surface'] in ('てる', 'でる'):
                end = i
            elif token['surface'] in ('て', 'で'):
                end = suffix_end(i)
        elif (token['surface'] in ('て', 'で')
              and (token['pos'][:2] == ['助詞', '接続助詞']
                   or (token['surface'] == 'で' and token['lemma'] == 'だ'
                       and token['pos'][:1] == ['助動詞'])) and has_next(tokens, i)):
            iru = tokens[i + 1]
            if iru['lemma'] in ('いる', '居る') and iru['pos'][:2] == ['動詞', '非自立可能']:
                if iru['surface'] in ('いる', '居る'):
                    end = i + 1
                elif iru['surface'] in ('い', '居'):
                    end = suffix_end(i + 1)
            elif (iru['lemma'] == 'ない' and iru['pos'][:2] == ['形容詞', '非自立可能']):
                end = i + 1
                if iru['surface'] == 'なかっ':
                    if (not has_next(tokens, end) or tokens[end + 1]['lemma'] != 'た'
                            or tokens[end + 1]['pos'][:1] != ['助動詞']):
                        continue
                    end += 1
            elif (iru['surface'] in ('ません', 'ませんでした')
                  and iru['pos'][:1] in (['接尾辞'], ['名詞'])):
                end = i + 1
        if end is not None:
            yield token['span'][0], tokens[end]['span'][1]
