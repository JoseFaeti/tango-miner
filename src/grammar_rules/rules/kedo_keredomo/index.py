"""Concessive and softening clause-final kedo forms."""
from src.GrammarConstants import KEDO_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_kedo(tokens):
    for i in range(1, len(tokens)):
        previous, marker = tokens[i - 1:i + 1]
        pos = previous['pos']
        if (adjacent(previous, marker)
                and pos[:1] in (['動詞'], ['形容詞'], ['助動詞'])
                and len(pos) >= 6 and pos[5].startswith('終止形')
                and marker['surface'] in ('けど', 'けれど', 'けれども')
                and marker['pos'][:2] == ['助詞', '接続助詞']):
            end = marker['span'][1]
            if marker['surface'] == 'けれど' and i + 1 < len(tokens):
                mo = tokens[i + 1]
                if (adjacent(marker, mo) and mo['surface'] == 'も' and mo['lemma'] == 'も'
                        and mo['pos'][:2] == ['助詞', '係助詞']):
                    end = mo['span'][1]
            yield marker['span'][0], end


RULE = GrammarRule(id='kedo_keredomo', expression=KEDO_EXPRESSION,
                   reading='けど / けれども', meaning='but; although; softening', match=match_kedo)
