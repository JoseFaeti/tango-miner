"""Rule for te_iru."""
from src.GrammarConstants import TE_IRU_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import polite_suffix_end, has_next, negative_suffix_end


RULE = GrammarRule(
    id='te_iru', expression=TE_IRU_GRAMMAR_EXPRESSION, reading='ている',
    meaning='be doing; be in a resulting state; do habitually',
    match=lambda tokens: match_te_iru(tokens),
)


def match_te_iru(tokens):
    def conditional_end(i):
        if (has_next(tokens, i) and tokens[i + 1]['surface'] == 'ば'
                and tokens[i + 1]['pos'][:2] == ['助詞', '接続助詞']):
            return i + 1
        return None

    def suffix_end(i):
        if not has_next(tokens, i):
            return None
        suffix = tokens[i + 1]
        if suffix['surface'] == 'て' and suffix['pos'][:2] == ['助詞', '接続助詞']:
            return i + 1
        if suffix['surface'] == 'たり' and suffix['pos'][:2] == ['助詞', '副助詞']:
            return i + 1
        if suffix['pos'][:1] != ['助動詞']:
            return None
        if suffix['lemma'] == 'た':
            return i + 1
        if suffix['lemma'] == 'ます':
            end = polite_suffix_end(tokens, i + 1)
            return end
        if suffix['lemma'] == 'ない':
            return negative_suffix_end(tokens, i + 1, pos='助動詞', continuative=True, extended=True)
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
            if token['surface'] in ('てる', 'でる', 'てよう', 'でよう', 'てろ', 'でろ'):
                end = i
            elif (token['surface'] in ('てん', 'でん') and has_next(tokens, i)
                  and tokens[i + 1]['surface'] == 'の'
                  and tokens[i + 1]['pos'][:2] in (['助詞', '準体助詞'], ['助詞', '終助詞'])):
                end = i
            elif token['surface'] in ('てれ', 'でれ'):
                end = conditional_end(i)
            elif token['surface'] in ('て', 'で'):
                end = suffix_end(i)
                if (end is None and has_next(tokens, i)
                        and tokens[i + 1]['surface'] == 'ん'
                        and tokens[i + 1]['pos'][:2] == ['助詞', '準体助詞']):
                    end = i + 1
        elif (token['surface'] in ('て', 'で')
              and (token['pos'][:2] == ['助詞', '接続助詞']
                   or (token['surface'] == 'で' and token['lemma'] == 'だ'
                       and token['pos'][:1] == ['助動詞'])) and has_next(tokens, i)):
            iru = tokens[i + 1]
            if iru['lemma'] in ('いる', '居る') and iru['pos'][:2] == ['動詞', '非自立可能']:
                if iru['surface'] in ('いる', '居る', 'いよう', '居よう', 'いろ', '居ろ'):
                    end = i + 1
                elif iru['surface'] in ('いれ', '居れ'):
                    end = conditional_end(i + 1)
                elif iru['surface'] in ('い', '居'):
                    end = suffix_end(i + 1)
            elif (iru['lemma'] == 'ない' and iru['pos'][:2] == ['形容詞', '非自立可能']):
                end = negative_suffix_end(tokens, i + 1, pos='形容詞', continuative=True, extended=True)
            elif (iru['surface'] in ('ません', 'ませんでした')
                  and iru['pos'][:1] in (['接尾辞'], ['名詞'])):
                end = i + 1
        if end is not None:
            yield token['span'][0], tokens[end]['span'][1]
