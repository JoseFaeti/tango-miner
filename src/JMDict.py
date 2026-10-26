import xml.etree.ElementTree as ET
import re

from pathlib import Path
from typing import List, Dict


# Priority bonuses (secondary to nfXX)
PRI_BONUS = {
    "ichi1": 1000,
    "ichi2": 500,
    "news1": 800,
    "news2": 400,
    "spec1": 600,
    "spec2": 300,
    "gai1": 100,
    "gai2": 50,
}

KANA_RE = re.compile(r"[ぁ-んァ-ンー]+")

# JMdict POS tags that represent nouns.
NOUN_POS = {
    "n",
    "n-adv",
    "n-pr",
    "n-pref",
    "n-suf",
    "n-t",
}


def is_noun_sense(sense: Dict) -> bool:
    """Return True if all POS tags of a sense are noun types."""
    return bool(sense["pos"]) and all(
        pos.startswith("noun")
        for pos in sense["pos"]
    )


def should_skip_short_kana_noun(word: str, entry: Dict) -> bool:
    """
    Skip 1-2 character kana-only words when every sense is a noun.
    """
    if len(word) >= 3:
        return False

    if KANA_RE.fullmatch(word) is None:
        return False

    return bool(entry["senses"]) and all(
        is_noun_sense(sense)
        for sense in entry["senses"]
    )


class JMDict:
    def __init__(self, xml_path: str):
        self.xml_path = Path(xml_path)
        self.tree = ET.parse(self.xml_path)
        self.root = self.tree.getroot()
        self.index = {}
        self._build_index()

    def _build_index(self):
        for entry in self.root.findall("entry"):
            entry_data = self._parse_entry(entry)
            for k in entry_data["kanji"]:
                self.index.setdefault(k["form"], []).append(entry_data)
            for r in entry_data["reading"]:
                self.index.setdefault(r["form"], []).append(entry_data)

    def _parse_entry(self, entry):
        k_list = []
        for k_ele in entry.findall("k_ele"):
            keb = k_ele.findtext("keb")
            pri_tags = [p.text for p in k_ele.findall("ke_pri")]
            k_list.append({"form": keb, "pri": pri_tags})

        r_list = []
        for r_ele in entry.findall("r_ele"):
            reb = r_ele.findtext("reb")
            pri_tags = [p.text for p in r_ele.findall("re_pri")]
            r_list.append({"form": reb, "pri": pri_tags})

        senses = []
        for sense in entry.findall("sense"):
            glosses = [g.text for g in sense.findall("gloss")]
            pos_list = [p.text for p in sense.findall("pos")]
            misc_list = [m.text for m in sense.findall("misc")]
            field_list = [f.text for f in sense.findall("field")]
            dialect_list = [d.text for d in sense.findall("dial")]
            senses.append({
                "gloss": glosses,
                "pos": pos_list,
                "misc": misc_list,
                "field": field_list,
                "dialect": dialect_list
            })

        return {"kanji": k_list, "reading": r_list, "senses": senses}

    def lookup_word(self, word: str) -> List[Dict]:
        """Return all entries for this word (raw structured JMdict data)."""
        result = self.index.get(word, [])

        # print(result)

        return result if isinstance(result, list) else [result]

    def get_best_entry(self, word: str) -> Dict:
        entries = self.lookup_word(word)

        if not entries:
            return None

        best_entry = self._best_entries(entries, word, tie_break="defs")[0]

        if should_skip_short_kana_noun(word, best_entry):
            # print("SKIPPING:", word)
            return None

        return best_entry

    def get_most_common_definition(self, word):
        best_entry = self.get_best_entry(word)

        if not best_entry:
            return None

        english_defs = []
        for i, s in enumerate(best_entry["senses"]):
            glosses = s["gloss"]
            if not glosses:
                continue
            index = f"{i+1}. " if len(best_entry["senses"]) > 1 else ""
            english_defs.append(f"{index}{'; '.join(glosses)}")

        return "<br>".join(english_defs)

    def get_reading(self, entry: Dict, word: str = None) -> str:
        """Return the most appropriate reading for a JMdict entry."""
        readings = entry["reading"]

        if not readings:
            return ""

        if word:
            for reading in readings:
                if reading["form"] == word:
                    return reading["form"]

        return readings[0]["form"]

    def has_non_noun_sense(self, definitions: list[dict]) -> bool:
        """Return True if any sense has a POS other than noun-only."""
        for definition in definitions:
            for sense in definition.get("senses", []):
                pos_tags = sense.get("pos", [])

                if not pos_tags:
                    continue

                if any(
                    not tag.startswith("noun")
                    for tag in pos_tags
                ):
                    return True

        return False


    def _best_entries(self, entries, search_word, tie_break="all"):
        if not entries:
            return []

        kana_only = KANA_RE.fullmatch(search_word) is not None

        def score_tags(tags):
            score = 0
            for tag in tags:
                if tag.startswith("nf"):
                    try:
                        n = int(tag[2:])
                        score += (49 - n) * 100
                    except ValueError:
                        continue
                elif tag in PRI_BONUS:
                    score += PRI_BONUS[tag]
            return score

        def score_entry(entry):
            score = 0
            matched = False
            for k in entry["kanji"]:
                if k["form"] == search_word:
                    score += score_tags(k.get("pri", []))
                    matched = True
            for r in entry["reading"]:
                if r["form"] == search_word:
                    score += score_tags(r.get("pri", []))
                    matched = True
            if not matched:
                for r in entry["reading"]:
                    score += score_tags(r.get("pri", []))
                    break
            return score

        scored = [(e, score_entry(e)) for e in entries]
        max_score = max(score for _, score in scored)
        top_entries = [e for e, score in scored if score == max_score]

        if tie_break == "all":
            return top_entries
        elif tie_break == "defs":
            return [max(top_entries, key=lambda e: len(e["senses"]))]
        else:
            raise ValueError("tie_break must be 'all' or 'defs'")