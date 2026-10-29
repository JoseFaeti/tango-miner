"""Explicit registry of grammar rules."""
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.rules.te_iru import RULE as TE_IRU
from src.grammar_rules.rules.nda import RULE as NDA
from src.grammar_rules.rules.tai import RULE as TAI
from src.grammar_rules.rules.te_shimau import RULE as TE_SHIMAU
from src.grammar_rules.rules.monono import RULE as MONONO
from src.grammar_rules.rules.nai import RULE as NAI
from src.grammar_rules.rules.you_ni import RULE as YOU_NI
from src.grammar_rules.rules.tara import RULE as TARA
from src.grammar_rules.rules.koto_ga_aru import RULE as KOTO_GA_ARU
from src.grammar_rules.rules.koto_ga_aru_occasional import RULE as KOTO_GA_ARU_OCCASIONAL

from src.grammar_rules.rules.te_kuru import RULE as TE_KURU
from src.grammar_rules.rules.te_iku import RULE as TE_IKU

RULES = (YOU_NI, KOTO_GA_ARU_OCCASIONAL, KOTO_GA_ARU, TE_IKU, TARA, NAI, TE_KURU, TE_IRU, NDA, TAI, TE_SHIMAU, MONONO,)

if frozenset(rule.expression for rule in RULES) != GRAMMAR_EXPRESSIONS:
    raise ValueError('Registered grammar identities and Anki protections must match')
