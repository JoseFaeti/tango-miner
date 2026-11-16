"""The unavoidable occurrence of an undesirable outcome."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_suffix_end, polite_suffix_end

OMANUGARENAI_EXPRESSION = '〜を免れない'


def match_omanugarenai(tokens):
    for i in range(1, len(tokens) - 2):
        noun = tokens[i - 1]
        particle, verb, negative = tokens[i:i + 3]
        if not (noun['pos'][:1] in (['名詞'], ['代名詞']) and adjacent(noun, particle)
                and adjacent(particle, verb) and adjacent(verb, negative)):
            continue
        if (particle['surface'], particle['lemma'], particle['pos'][:1]) != ('を', 'を', ['助詞']):
            continue
        if not (verb['lemma'] in ('免れる', 'まぬがれる', 'まぬかれる') and verb['pos'][:1] == ['動詞']
                and verb['surface'] in ('免れ', 'まぬがれ', 'まぬかれ')):
            continue
        end = negative_suffix_end(tokens, i + 2, pos='助動詞', continuative=True, conditional=True)
        if end is None and negative['lemma'] == 'ます':
            end = polite_suffix_end(tokens, i + 2, forms=('negative',))
        if end is not None:
            yield particle['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='omanugarenai', expression=OMANUGARENAI_EXPRESSION,
                   reading='をまぬがれない', meaning='cannot escape; cannot avoid',
                   match=match_omanugarenai, order=1)
RULES = (RULE,)
