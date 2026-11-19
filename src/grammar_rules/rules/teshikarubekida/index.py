"""Expected or deserved outcomes with てしかるべき."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def match_teshikarubekida(tokens):
    for i in range(1, len(tokens) - 2):
        previous, connective, shikaru, beki = tokens[i - 1:i + 3]
        if (previous['pos'][:1] not in (['動詞'], ['助動詞'])
                or len(previous['pos']) < 6 or not previous['pos'][5].startswith('連用形')
                or connective['surface'] not in ('て', 'で')
                or connective['pos'][:2] != ['助詞', '接続助詞']
                or shikaru['surface'] not in ('しかる', '然る')
                or shikaru['lemma'] not in ('しかり', '然り')
                or shikaru['pos'][:1] != ['動詞']
                or (beki['surface'], beki['lemma'], beki['pos'][:1]) != ('べき', 'べし', ['助動詞'])
                or not all(adjacent(a, b) for a, b in zip(
                    (previous, connective, shikaru), (connective, shikaru, beki)))):
            continue
        end = i + 2
        if end + 1 < len(tokens) and adjacent(beki, tokens[end + 1]):
            copula = tokens[end + 1]
            if (copula['lemma'], copula['surface'], copula['pos'][:1]) in (
                    ('だ', 'だ', ['助動詞']), ('です', 'です', ['助動詞'])):
                end += 1
            elif (copula['lemma'], copula['surface'], copula['pos'][:1]) in (
                    ('だ', 'だっ', ['助動詞']), ('です', 'でし', ['助動詞'])):
                if not next_matches(tokens, end + 1, 'た', '助動詞', 'た'):
                    continue
                end += 2
        yield connective['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='teshikarubekida', expression='〜てしかるべきだ',
                   reading='てしかるべきだ', meaning='naturally should; deserve to',
                   match=match_teshikarubekida, order=131)
RULES = (RULE,)
