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
                "stagk": [s.text for s in sense.findall("stagk")],
                "stagr": [s.text for s in sense.findall("stagr")],
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

    def get_potential_definition(self, word, component_counts, tokenizer):
        """Describe a source-grounded potential form using its base verb."""
        if not component_counts or not word.endswith("る"):
            return None
        has_literal = False
        for components, count in component_counts.items():
            if count <= 0 or len(components) != 1:
                return None
            token = components[0]
            if not token.pos or token.pos[0] != "動詞" or token.lemma != word:
                return None
            has_literal |= token.surface == word
        if not has_literal:
            return None
        from sudachipy import tokenizer as sudachi_tokenizer
        nodes = tokenizer.tokenize(word, sudachi_tokenizer.Tokenizer.SplitMode.C)
        if len(nodes) != 1 or nodes[0].dictionary_form() != word:
            return None
        base = nodes[0].normalized_form()
        endings = {"う": "える", "く": "ける", "ぐ": "げる", "す": "せる",
                   "つ": "てる", "ぬ": "ねる", "ぶ": "べる", "む": "める", "る": "れる"}
        if not base or base[-1] not in endings or base[:-1] + endings[base[-1]] != word:
            return None
        readings = {components[0].reading for components in component_counts}
        suffix = endings[base[-1]]
        if len(readings) != 1:
            return None
        reading, = readings
        if not reading.endswith(suffix):
            return None
        base_reading = reading[:-len(suffix)] + base[-1]
        entries = []
        for entry in self.lookup_word(base):
            if not any(r["form"] == base_reading for r in entry["reading"]):
                continue
            senses = []
            pos = []
            for sense in entry["senses"]:
                if sense["pos"]:
                    pos = sense["pos"]
                if sense["gloss"] and any("verb" in tag.lower() and not tag.lower().startswith(("adverb", "noun", "auxiliary")) for tag in pos):
                    senses.append(sense)
            if senses:
                entries.append(senses)
        if len(entries) != 1:
            return None
        glosses = ["; ".join(sense["gloss"]) for sense in entries[0]]
        definition = "<br>".join(f"{i + 1}. {gloss}" if len(glosses) > 1 else gloss
                                  for i, gloss in enumerate(glosses))
        return f"Potential form of {base}: {definition}"

    def get_contextual_short_entry(self, word, component_counts):
        """Use unanimous single-token POS evidence only when it identifies one entry."""
        if not (len(word) <= 2 and KANA_RE.fullmatch(word) and component_counts):
            return None
        categories = set()
        for components, count in component_counts.items():
            if count <= 0 or len(components) != 1 or not components[0].pos:
                return None
            category = {"助動詞": "auxiliary", "連体詞": "pre-noun adjectival"}.get(components[0].pos[0])
            if category is None:
                return None
            categories.add(category)
        if len(categories) != 1:
            return None
        category, = categories
        def compatible(tag):
            tag = tag.lower()
            if category == "verb":
                return "verb" in tag and not tag.startswith("adverb") and not tag.startswith("auxiliary") and not tag.startswith("noun")
            return tag.startswith(category)
        matches = []
        for entry in self.lookup_word(word):
            senses = []
            inherited_pos = []
            for sense in entry["senses"]:
                if sense["pos"]:
                    inherited_pos = sense["pos"]
                if sense["gloss"] and any(compatible(tag) for tag in inherited_pos):
                    senses.append(sense)
            if senses:
                matches.append((entry, senses))
        if len(matches) != 1:
            return None
        entry, senses = matches[0]
        return {**entry, "senses": senses}

    def get_contextual_modifier_entry(self, word, component_counts, best_entry):
        """Correct a POS-incompatible entry only with unanimous adjective evidence."""
        if not component_counts or not best_entry:
            return None
        categories, readings = set(), set()
        for components, count in component_counts.items():
            if count <= 0 or len(components) != 1:
                return None
            component = components[0]
            if component.lemma != word or not component.pos or not component.reading:
                return None
            category = {"形容詞": "adjective (keiyoushi)"}.get(component.pos[0])
            if category is None:
                return None
            categories.add(category)
            readings.add(component.reading)
        if len(categories) != 1 or len(readings) != 1:
            return None
        category, = categories
        reading, = readings

        def compatible(entry):
            return any(tag.lower().startswith(category)
                       for sense in entry['senses'] for tag in sense['pos'])

        # Do not replace one plausible homophone with another based on POS.
        if compatible(best_entry):
            return None
        matches = [entry for entry in self.lookup_word(word)
                   if compatible(entry) and any(r['form'] == reading for r in entry['reading'])]
        if len(matches) != 1:
            return None
        entry = matches[0]
        senses = []
        inherited_pos = []
        for sense in entry['senses']:
            if sense['pos']:
                inherited_pos = sense['pos']
            if any(tag.lower().startswith(category) for tag in inherited_pos):
                senses.append(sense)
        return {**entry, 'senses': senses}

    def select_contextual_entry(self, word, component_counts):
        """Return an entry and supported analyses, or None when evidence is inconclusive.

        A token analysis supports an entry only when it uniquely identifies it;
        frequency ranks supported entries, never unresolved homophones.
        """
        entries = self.lookup_word(word)
        if len(entries) < 2 or not component_counts:
            return None
        scores = {}
        supported = {}
        selected_senses = {}
        for components, count in component_counts.items():
            if count <= 0 or len(components) != 1:
                continue
            token = components[0]
            if token.lemma != word or not token.pos or not token.reading:
                continue
            pos = token.pos[0]
            category = {"動詞": "verb", "形容詞": "adjective (keiyoushi)",
                        "副詞": "adverb", "名詞": "noun",
                        "助動詞": "auxiliary", "連体詞": "pre-noun adjectival"}.get(pos)
            if not category:
                continue
            conjugation = token.pos[4] if len(token.pos) > 4 else ""
            matches = []
            for index, entry in enumerate(entries):
                if not any(r['form'] == token.reading for r in entry['reading']):
                    continue
                senses, inherited = [], []
                for sense in entry['senses']:
                    if sense['pos']:
                        inherited = sense['pos']
                    tags = [p.lower() for p in inherited]
                    if category == 'verb':
                        compatible = any('verb' in tag and not tag.startswith(('adverb', 'noun', 'auxiliary')) for tag in tags)
                        if conjugation.startswith('サ行変格'):
                            compatible = any(tag.startswith('suru verb') for tag in tags)
                        elif conjugation.startswith('五段'):
                            compatible = any(tag.startswith('godan verb') for tag in tags)
                        elif conjugation.startswith(('上一段', '下一段')):
                            compatible = any(tag.startswith('ichidan verb') for tag in tags)
                    else:
                        compatible = any(tag.startswith(category) for tag in tags)
                    if compatible and sense['gloss']:
                        senses.append(sense)
                if senses:
                    matches.append((index, senses))
            # Non-independent ichidan verbs often mark an auxiliary use. Only
            # narrow by this cue when exactly one compatible entry has it.
            if len(matches) > 1 and category == 'verb' and len(token.pos) > 1 and token.pos[1] == '非自立可能':
                auxiliary = [(i, senses) for i, senses in matches if any(
                    tag.lower().startswith('auxiliary verb')
                    for sense in entries[i]['senses'] for tag in sense['pos'])]
                if len(auxiliary) == 1:
                    matches = auxiliary
            if len(matches) != 1:
                continue
            index, senses = matches[0]
            scores[index] = scores.get(index, 0) + count
            supported.setdefault(index, set()).add(components)
            selected_senses.setdefault(index, {})[tuple(id(s) for s in senses)] = senses
        if not scores:
            return None
        ranking = sorted(scores.items(), key=lambda item: item[1], reverse=True)
        if len(ranking) > 1 and ranking[0][1] == ranking[1][1]:
            return None
        index = ranking[0][0]
        allowed = {id(s) for senses in selected_senses[index].values() for s in senses}
        entry = entries[index]
        return ({**entry, 'senses': [s for s in entry['senses'] if id(s) in allowed]}, supported[index])

    def get_most_common_definition(self, word, *, component_counts=None):
        selection = self.select_contextual_entry(word, component_counts)
        if selection is None and component_counts and len(self.lookup_word(word)) > 1:
            return None
        best_entry = selection[0] if selection else self.get_best_entry(word)
        contextual_entry = self.get_contextual_modifier_entry(word, component_counts, best_entry)
        if contextual_entry is not None:
            best_entry = contextual_entry
        if best_entry is None and component_counts:
            best_entry = self.get_contextual_short_entry(word, component_counts)

        if not best_entry:
            return None

        # Only apply a restriction for the dimension actually known from the
        # lookup. A kanji lookup alone does not establish which reading was
        # intended, and a kana lookup does not establish a kanji spelling.
        is_kanji_form = any(k["form"] == word for k in best_entry["kanji"])
        is_reading_form = any(r["form"] == word for r in best_entry["reading"])
        senses = [s for s in best_entry["senses"]
                  if (not is_kanji_form or not s.get("stagk") or word in s["stagk"])
                  and (not is_reading_form or not s.get("stagr") or word in s["stagr"])
                  and s["gloss"]]
        english_defs = []
        for i, s in enumerate(senses):
            glosses = s["gloss"]
            if not glosses:
                continue
            index = f"{i+1}. " if len(senses) > 1 else ""
            english_defs.append(f"{index}{'; '.join(glosses)}")

        return "<br>".join(english_defs) or None

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
