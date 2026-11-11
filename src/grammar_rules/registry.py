from src.grammar_rules.rules.da_desu.index import RULE as DA_DESU
from src.grammar_rules.rules.datta_deshita.index import RULE as DATTA_DESHITA
from src.grammar_rules.rules.janai_dewanai.index import RULE as JANAI_DEWANAI
from src.grammar_rules.rules.ne_yo_yone.index import RULE as NE_YO_YONE
from src.grammar_rules.rules.sae_sura.index import RULE as SAE_SURA
from src.grammar_rules.rules.nado_nanka_nante.index import RULE as NADO_NANKA_NANTE
from src.grammar_rules.rules.hodo_kurai_gurai.index import RULE as HODO_KURAI_GURAI
from src.grammar_rules.rules.shikanai.index import RULE as SHIKANAI
from src.grammar_rules.rules.ya_toka.index import RULE as YA_TOKA
from src.grammar_rules.rules.dake_nomi.index import RULE as DAKE_NOMI
from src.grammar_rules.rules.toki.index import RULE as TOKI
from src.grammar_rules.rules.ha.index import RULE as HA
from src.grammar_rules.rules.ga.index import RULE as GA
from src.grammar_rules.rules.mitai_ni.index import RULE as MITAI_NI
from src.grammar_rules.rules.tsupoi.index import RULE as TSUPOI
from src.grammar_rules.rules.naosu.index import RULE as NAOSU
from src.grammar_rules.rules.madeni.index import RULE as MADENI
from src.grammar_rules.rules.you_to_suru.index import RULE as YOU_TO_SURU
from src.grammar_rules.rules.toiu.index import RULE as TOIU
from src.grammar_rules.rules.tte.index import RULE as TTE
from src.grammar_rules.rules.you_to_omou.index import RULE as YOU_TO_OMOU
from src.grammar_rules.rules.you_ni_suru.index import RULE as YOU_NI_SURU
"""Explicit registry of grammar rules."""
from src.grammar_rules.rules.you_ni_naru.index import RULE as YOU_NI_NARU
from src.grammar_rules.rules.tsumorida.index import RULE as TSUMORIDA
from src.grammar_rules.rules.tabakari.index import RULE as TABAKARI
from src.grammar_rules.rules.bakari.index import RULE as BAKARI
from src.grammar_rules.rules.tokoroda.index import RULE as TOKORODA
from src.grammar_rules.rules.uchini.index import RULE as UCHINI
from src.grammar_rules.rules.maeni_atode.index import RULE as MAENI_ATODE
from src.grammar_rules.rules.kedo_keredomo.index import RULE as KEDO
from src.grammar_rules.rules.taritarisuru.index import RULE as TARITARISURU
from src.grammar_rules.rules.kara_node.index import RULE as KARA_NODE
from src.grammar_rules.rules.kara_made.index import RULE as KARA_MADE
from src.grammar_rules.rules.te_kara.index import RULE as TE_KARA
from src.grammar_rules.rules.te_benefactive.index import RULE as TE_BENEFACTIVE
from src.grammar_rules.rules.naide.index import RULE as NAIDE
from src.grammar_rules.rules.kamoshirenai.index import RULE as KAMOSHIRENAI
from src.grammar_rules.rules.te_kudasai.index import RULE as TE_KUDASAI
from src.grammar_rules.rules.noni.index import RULE as NONI
from src.grammar_rules.rules.tame_ni.index import RULE as TAME_NI
from src.grammar_rules.rules.nagara.index import RULE as NAGARA
from src.grammar_rules.rules.wake_da.index import EXPLANATION as WAKE_DA, NEGATIVE as WAKE_NEGATIVE
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.rules.hazu_da.index import RULE as HAZU_DA
from src.grammar_rules.rules.sou_da.index import APPEARANCE, HEARSAY
from src.grammar_rules.rules.te_wa_ikenai.index import RULE as TE_WA_IKENAI
from src.grammar_rules.rules.te_miru.index import RULE as TE_MIRU
from src.grammar_rules.rules.koto_ni_suru.index import RULE as KOTO_NI_SURU
from src.grammar_rules.rules.koto_ni_naru.index import RULE as KOTO_NI_NARU
from src.grammar_rules.rules.te_oku.index import RULE as TE_OKU
from src.grammar_rules.rules.te_aru.index import RULE as TE_ARU
from src.grammar_rules.rules.te_mo.index import RULE as TE_MO
from src.grammar_rules.rules.nakutemoii.index import RULE as NAKUTEMOII
from src.grammar_rules.rules.te_mo_ii.index import RULE as TE_MO_II
from src.grammar_rules.rules.obligation.index import RULE as OBLIGATION
from src.grammar_rules.rules.elliptical_obligation.index import RULE as ELLIPTICAL_OBLIGATION
from src.grammar_rules.rules.to_omou.index import RULE as TO_OMOU
from src.grammar_rules.rules.te_iru.index import RULE as TE_IRU
from src.grammar_rules.rules.te_hoshii.index import RULE as TE_HOSHII
from src.grammar_rules.rules.nda.index import RULE as NDA
from src.grammar_rules.rules.tagaru.index import RULE as TAGARU
from src.grammar_rules.rules.tai.index import RULE as TAI
from src.grammar_rules.rules.te_shimau.index import RULE as TE_SHIMAU
from src.grammar_rules.rules.monono.index import RULE as MONONO
from src.grammar_rules.rules.nai.index import RULE as NAI
from src.grammar_rules.rules.you_da.index import RULE as YOU_DA
from src.grammar_rules.rules.you_ni.index import RULE as YOU_NI
from src.grammar_rules.rules.nara.index import RULE as NARA
from src.grammar_rules.rules.ba.index import RULE as BA
from src.grammar_rules.rules.tara.index import RULE as TARA
from src.grammar_rules.rules.koto_ga_aru.index import RULE as KOTO_GA_ARU
from src.grammar_rules.rules.koto_ga_aru_occasional.index import RULE as KOTO_GA_ARU_OCCASIONAL
from src.grammar_rules.rules.te_kuru.index import RULE as TE_KURU
from src.grammar_rules.rules.te_iku.index import RULE as TE_IKU
from src.grammar_rules.rules.ta_hou_ga_ii.index import RULE as TA_HOU_GA_II
from src.grammar_rules.rules.koto_ga_dekiru.index import RULE as KOTO_GA_DEKIRU
from src.grammar_rules.rules.yasui_nikui.index import RULE as YASUI_NIKUI
from src.grammar_rules.rules.yori.index import RULE as YORI

RULES = (NAGARA, TAGARU, WAKE_DA, WAKE_NEGATIVE, HAZU_DA, APPEARANCE, HEARSAY, TE_MO, NARA, BA, YOU_DA, KOTO_NI_SURU, KOTO_NI_NARU, TE_WA_IKENAI, TE_MIRU, TE_OKU, TE_MO_II, OBLIGATION, TO_OMOU, YOU_NI, KOTO_GA_ARU_OCCASIONAL, KOTO_GA_ARU, TE_IKU, TARA, NAI, TE_KURU, TE_IRU, NDA, TAI, TE_SHIMAU, MONONO,)

RULES = (MITAI_NI, TSUPOI, NAOSU, MADENI, TOKI, TOIU, TTE, YOU_TO_OMOU, YOU_TO_SURU, YOU_NI_SURU, YOU_NI_NARU, TSUMORIDA, TABAKARI, TOKORODA, UCHINI, TE_KARA, MAENI_ATODE, KEDO, KARA_NODE, TARITARISURU, YASUI_NIKUI, KOTO_GA_DEKIRU, TA_HOU_GA_II, ELLIPTICAL_OBLIGATION, NAKUTEMOII, TE_HOSHII, TE_BENEFACTIVE, NAIDE, TE_KUDASAI, KAMOSHIRENAI, NONI, TAME_NI, TE_ARU,) + RULES
RULES = (NE_YO_YONE, SHIKANAI, SAE_SURA, NADO_NANKA_NANTE, DAKE_NOMI, HODO_KURAI_GURAI, YA_TOKA, GA, HA, KARA_MADE, YORI, BAKARI,) + RULES
RULES = RULES + (DA_DESU, DATTA_DESHITA, JANAI_DEWANAI,)

if frozenset(rule.expression for rule in RULES) != GRAMMAR_EXPRESSIONS:
    raise ValueError('Registered grammar identities and Anki protections must match')
