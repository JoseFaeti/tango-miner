"""Completion or inability to complete a continuative verb."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, negative_suffix_end, polite_suffix_end

KIRU_KIRENAI_EXPRESSION = '〜きる / 〜きれない'


def match_kiru_kirenai(tokens):
    for i, token in enumerate(tokens):
        lemma = token['lemma']
        if token['pos'][:1] != ['動詞'] or not isinstance(lemma, str):
            continue
        suffix = next((s for s in ('きれる', '切れる', 'きる', '切る') if lemma.endswith(s)), None)
        if suffix is None:
            continue
        if lemma == suffix:
            if not i:
                continue
            previous = tokens[i - 1]
            if not (adjacent(previous, token) and previous['pos'][:1] == ['動詞']
                    and len(previous['pos']) >= 6 and previous['pos'][5].startswith('連用形')):
                continue
            start = previous['span'][0]
        else:
            # Kana-only compounds can include unrelated verbs such as 生きる.
            if suffix.startswith('き'):
                continue
            start = token['span'][0]
        surface = token['surface']
        potential = suffix.endswith('れる')
        end = None
        if potential:
            if surface.endswith(suffix[:-1]):
                if next_matches(tokens, i, 'ない', '助動詞'):
                    end = negative_suffix_end(tokens, i + 1, pos='助動詞', extended=True)
                elif next_matches(tokens, i, 'ます', '助動詞'):
                    end = polite_suffix_end(tokens, i + 1, forms=('negative',))
        elif surface.endswith(suffix):
            end = i
        elif surface.endswith(suffix[:-1] + 'っ'):
            if next_matches(tokens, i, 'た', '助動詞', 'た') or next_matches(tokens, i, 'て', '助詞', 'て'):
                end = i + 1
        elif surface.endswith(suffix[:-1] + 'り') and next_matches(tokens, i, 'ます', '助動詞'):
            end = polite_suffix_end(tokens, i + 1, forms=('positive',))
        elif surface.endswith(suffix[:-1] + 'れ') and next_matches(tokens, i, 'ば', '助詞', 'ば'):
            end = i + 1
        if end is not None:
            yield start, tokens[end]['span'][1]


RULE = GrammarRule(order=182, id='kiru_kirenai', expression=KIRU_KIRENAI_EXPRESSION,
                   reading='きる / きれない', meaning='do completely; be unable to finish doing',
                   match=match_kiru_kirenai)
RULES = (RULE,)
