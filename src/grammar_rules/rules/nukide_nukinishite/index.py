"""Without or leaving something out: 抜きで / 抜きにして."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import next_matches

NUKIDE_NUKINISHITE_GRAMMAR_EXPRESSION = '〜ぬきで / 〜ぬきにして'


def match_nukide_nukinishite(tokens):
    for i, token in enumerate(tokens):
        if (token['surface'] not in ('抜き', 'ぬき')
                or token['pos'][:1] != ['名詞']):
            continue
        if next_matches(tokens, i, 'で', '助詞', 'で'):
            end = i + 1
        elif (next_matches(tokens, i, 'に', '助詞', 'に')
              and next_matches(tokens, i + 1, 'する', '動詞', 'し')
              and next_matches(tokens, i + 2, 'て', '助詞', 'て')):
            end = i + 3
        else:
            continue
        yield token['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(
    id='nukide_nukinishite', expression=NUKIDE_NUKINISHITE_GRAMMAR_EXPRESSION,
    reading='ぬきで / ぬきにして', meaning='without; leaving out',
    match=match_nukide_nukinishite,
)
RULES = (RULE,)
