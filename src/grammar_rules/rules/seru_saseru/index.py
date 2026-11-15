"""Source-aligned causative auxiliary."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

SERU_SASERU_EXPRESSION = '〜せる / 〜させる'

RULE = GrammarRule(order=164, id='seru_saseru', expression=SERU_SASERU_EXPRESSION,
                   reading='せる / させる', meaning='make or let someone do',
                   match=lambda tokens: match_seru_saseru(tokens))


def match_seru_saseru(tokens):
    for i in range(1, len(tokens)):
        verb, suffix = tokens[i - 1:i + 1]
        if (verb['pos'][:1] == ['動詞'] and len(verb['pos']) >= 6
                and verb['pos'][5].startswith(('未然形', '連用形'))
                and suffix['pos'][:1] == ['助動詞']
                and suffix['lemma'] in ('せる', 'させる')
                and suffix['surface'] in ('せる', 'せ', 'せれ', 'せよ',
                                          'させる', 'させ', 'させれ', 'させよ')
                and adjacent(verb, suffix)):
            if (i + 1 < len(tokens) and adjacent(suffix, tokens[i + 1])
                    and tokens[i + 1]['pos'][:1] == ['助動詞']
                    and tokens[i + 1]['lemma'] == 'られる'):
                continue
            yield suffix['span'][0], suffix['span'][1]


RULES = (RULE,)
