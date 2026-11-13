"""Source-aligned focus particles さえ and すら."""
SAE_SURA_EXPRESSION = 'さえ / すら'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_sae_sura(tokens):
    for i in range(1, len(tokens)):
        previous, particle = tokens[i - 1:i + 1]
        if (particle['surface'] not in ('さえ', 'すら')
                or particle['lemma'] != particle['surface']
                or particle['pos'][:2] != ['助詞', '副助詞']
                or not adjacent(previous, particle)
                or previous['pos'][:1] == ['空白']):
            continue
        if previous['pos'][:1] == ['補助記号'] and previous['pos'][1:2] != ['括弧閉']:
            continue
        yield tuple(particle['span'])


RULE = GrammarRule(order=46, id='sae_sura', expression=SAE_SURA_EXPRESSION,
                   reading='さえ / すら', meaning='even', match=match_sae_sura)

RULES = (RULE,)
