import xml.etree.ElementTree as ET
import appdirs
import csv
import shelve
import re

from pathlib import Path
from typing import List, Dict

from .Artifact import Artifact
from .PipelineStep import PipelineStep
from .ProcessingStep import ProcessingStep

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
NO_DEFINITION = object()


# --- Cache helpers with automatic flushing ---
FLUSH_INTERVAL = 10  # flush every X definitions

cache = None
_pending_writes = None
_write_count = 0


class JMdictLookup:
    def __init__(self, xml_path: str):
        # Resolve path relative to this script
        self.xml_path = Path(xml_path)
        self.tree = ET.parse(self.xml_path)
        self.root = self.tree.getroot()

    def lookup_word(self, word: str) -> List[Dict]:
        results = []
        for entry in self.root.findall("entry"):
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

            # skip if word not found
            if not any(word == k["form"] for k in k_list) and not any(word == r["form"] for r in r_list):
                continue

            senses = []
            for sense in entry.findall("sense"):
                glosses = [g.text for g in sense.findall("gloss")]
                pos_list = [p.text for p in sense.findall("pos")]
                senses.append({"gloss": glosses, "pos": pos_list})

            results.append({
                "kanji": k_list,   # list of dicts: {"form": ..., "pri": [...]}
                "reading": r_list, # same
                "senses": senses
            })
        return results

    def lookup_list(self, words: List[str]) -> Dict[str, List[Dict]]:
        """
        Lookup a list of words and return a dict mapping word -> entries.
        """
        output = {}
        for word in words:
            output[word] = self.lookup_word(word)
        return output

# --- JMdict Lookup setup ---
xml_file = Path.home() / "JMdict_e.xml"
jmdict = None


def open_cache():
    """Initialize the shelve cache."""
    global cache, _pending_writes, _write_count
    if cache is not None:
        return

    cache_dir = Path(appdirs.user_cache_dir("tango_miner"))
    cache_dir.mkdir(parents=True, exist_ok=True)

    cache = shelve.open(str(cache_dir / "definitions-jmdict.db"), writeback=False)
    _pending_writes = {}
    _write_count = 0

def get_cached_definition(word):
    """Return a cached definition if available, else None."""
    try:
        return cache[word]
    except KeyError:
        return None

def cache_definition(word, definition):
    """Store a definition and flush periodically."""
    global _write_count
    _pending_writes[word] = definition
    _write_count += 1

    if _write_count >= FLUSH_INTERVAL:
        flush_cache()

def close_cache():
    for k, v in _pending_writes.items():
        cache[k] = v
    cache.close()


def flush_cache():
    """Write pending definitions to the shelve database."""
    global _pending_writes, _write_count
    if not _pending_writes:
        return
    for k, v in _pending_writes.items():
        cache[k] = v
    _pending_writes.clear()
    _write_count = 0


def close_cache():
    """Flush any remaining definitions and close the cache."""
    flush_cache()
    cache.close()


def normalize_word(word: str) -> str:
    return word.strip()


class AddDefinitionsStep(PipelineStep):
    def process(self, artifact: Artifact) -> Artifact:
        open_cache()
        data = add_and_filter_for_definitions(artifact.data, self.progress)
        close_cache()
        return Artifact(data)


def add_and_filter_for_definitions(input: dict, progress_handler):
    total = len(input)
    kept = {}
    total_invalid = 0

    progress_handler(ProcessingStep.DEFINITIONS, 0, total, 'Initializing dictionary...')

    global jamdict
    jamdict = JMdictLookup(xml_file)

    for i, word in enumerate(input):
        word_norm = normalize_word(word)
        definition = get_cached_definition(word_norm)

        if definition is None:
            definition = get_most_common_definition(word_norm) or ""
            # print(word_norm, definition)

            cache_definition(word_norm, definition)

        stats = input[word]
        stats.definition = definition
        stats.invalid = not definition
        kept[word] = stats

        if stats.invalid:
            total_invalid += 1

        progress_handler(ProcessingStep.DEFINITIONS, i, total)

    progress_handler(ProcessingStep.DEFINITIONS, 1, 1, f'{len(kept) - total_invalid} words kept')

    return kept


def get_most_common_definition(word: str, pos=None) -> str:
    global jmdict
    entries = jmdict.lookup_word(word)

    if not entries:
        return None  # no entries at all

    # Pick the best entry using your scoring
    best_entry = best_entries(entries, word, tie_break="defs")[0]

    # Collect all glosses from all senses
    english_defs = []
    for i, s in enumerate(best_entry["senses"]):
        glosses = s["gloss"]  # list of strings
        if not glosses:
            continue
        index = f"{i+1}. " if len(best_entry["senses"]) > 1 else ""
        english_defs.append(f"{index}{'; '.join(glosses)}")

    final_definition = "<br>".join(english_defs)
    return final_definition


def best_entries(entries, search_word, tie_break="all"):
    if not entries:
        return []

    kana_only = KANA_RE.fullmatch(search_word) is not None

    def score_tags(tags):
        score = 0
        for tag in tags:
            if tag.startswith("nf"):
                try:
                    n = int(tag[2:])
                    # nf01 highest → strong primary signal
                    score += (49 - n) * 100
                except ValueError:
                    continue
            elif tag in PRI_BONUS:
                score += PRI_BONUS[tag]
            # unknown tags ignored
        return score

    def score_entry(entry):
        score = 0
        matched = False

        # Score only matching kanji forms
        for k in entry["kanji"]:
            if k["form"] == search_word:
                score += score_tags(k.get("pri", []))
                matched = True

        # Score only matching reading forms
        for r in entry["reading"]:
            if r["form"] == search_word:
                score += score_tags(r.get("pri", []))
                matched = True

        # If nothing matched explicitly (rare edge case), fallback
        if not matched:
            # minimal fallback so entry isn't zeroed out
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
        # More senses usually correlates with common core meanings
        return [max(top_entries, key=lambda e: len(e["senses"]))]
    else:
        raise ValueError("tie_break must be 'all' or 'defs'")
