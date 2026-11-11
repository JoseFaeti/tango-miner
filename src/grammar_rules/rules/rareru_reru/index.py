"""Source-aligned potential/passive auxiliary, without semantic disambiguation."""
from src.GrammarConstants import RARERU_RERU_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

RULE = GrammarRule(id='rareru_reru', expression=RARERU_RERU_EXPRESSION,
                   reading='られる / れる',
                   meaning='can do; also passive or honorific (context dependent)',
                   match=lambda tokens: match_rareru_reru(tokens))


def match_rareru_reru(tokens):
    for i in range(1, len(tokens)):
        verb, suffix = tokens[i - 1:i + 1]
        if (verb['pos'][:1] == ['動詞'] and len(verb['pos']) >= 6
                and verb['pos'][5].startswith('未然形')
                and suffix['pos'][:1] == ['助動詞']
                and suffix['lemma'] in ('られる', 'れる')
                and suffix['surface'] in ('られる', 'られ', 'られれ', 'られよ',
                                          'れる', 'れ', 'れれ', 'れよ')
                and adjacent(verb, suffix)):
            yield suffix['span'][0], suffix['span'][1]
