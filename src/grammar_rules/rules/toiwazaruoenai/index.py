"""An unavoidable characterization expressed with と言わざるを得ない."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.zaruoenai.index import match_zaruoenai


def match_toiwazaruoenai(tokens):
    endings = dict(match_zaruoenai(tokens))
    for i in range(1, len(tokens) - 2):
        previous, quote, verb, zaru = tokens[i - 1:i + 3]
        if (previous['pos'][:1] != ['補助記号']
                and (quote['surface'], quote['lemma'], quote['pos'][:1]) == ('と', 'と', ['助詞'])
                and verb['surface'] in ('言わ', 'いわ')
                and verb['lemma'] in ('言う', 'いう') and verb['pos'][:1] == ['動詞']
                and all(adjacent(a, b) for a, b in zip((previous, quote, verb), (quote, verb, zaru)))
                and zaru['span'][0] in endings):
            yield quote['span'][0], endings[zaru['span'][0]]


RULE = GrammarRule(id='toiwazaruoenai', expression='〜と言わざるを得ない',
                   reading='といわざるをえない', meaning='have to say; cannot help but conclude',
                   match=match_toiwazaruoenai, order=70)
RULES = (RULE,)
