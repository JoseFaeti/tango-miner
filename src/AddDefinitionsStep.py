
import appdirs
import csv
import shelve
import re

from pathlib import Path

from .Artifact import Artifact
from .PipelineStep import PipelineStep
from .ProcessingStep import ProcessingStep

# Common priority tags — higher = more common
PRI_WEIGHTS = {
    "ichi1": 10000,
    "news1": 800,
    "spec1": 15000,
    "gai1": 10,
    "ichi2": 5000,
    "news2": 50,
    "spec2": 8000,
    "gai2": 9,
    # "nfXX" tags handled dynamically
}

KANA_RE = re.compile(r"[ぁ-んァ-ンー]+")
NO_DEFINITION = object()


cache = None
_pending_writes = None

def open_cache():
    global cache, _pending_writes
    if cache is not None:
        return

    cache_dir = Path(appdirs.user_cache_dir("tango_miner"))
    cache_dir.mkdir(parents=True, exist_ok=True)

    cache = shelve.open(str(cache_dir / "definitions.db"), writeback=False)
    _pending_writes = {}

def get_cached_definition(word):
    try:
        return cache[word]
    except KeyError:
        return None

def cache_definition(word, definition):
    _pending_writes[word] = definition

def close_cache():
    for k, v in _pending_writes.items():
        cache[k] = v
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

    progress_handler(ProcessingStep.DEFINITIONS, 0, total)

    for i, word in enumerate(input):
        word_norm = normalize_word(word)
        definition = get_cached_definition(word_norm)

        if definition is None:
            definition = get_most_common_definition(word_norm) or ""
            cache_definition(word_norm, definition)

        if definition:
            stats = input[word]
            stats.definition = definition
            kept[word] = stats

        progress_handler(ProcessingStep.DEFINITIONS, i, total)

    progress_handler(ProcessingStep.DEFINITIONS, 1, 1, f'{len(kept)} words kept')

    return kept


def get_most_common_definition(word: str) -> str:
    result = get_jamdict().lookup(word)

    if not result.entries:
        return ""

    # Pick the entry with the highest score
    # print_debug(result.entries)
    best_entries_result = best_entries(result.entries, word, tie_break="defs")

    if len(best_entries_result) == 0: return

    best_entry = best_entries_result[0] #max(result.entries, key=entry_rank)

    # Get the top 2 English glosses
    english_defs = []

    for i, s in enumerate(best_entry.senses):
        glosses = []

        if hasattr(s, "gloss"):
            glosses += (g.text for g in s.gloss)
        
        index = f'{i+1}. ' if len(best_entry.senses) > 1 else ''
        
        english_defs.append(f'{index}{"; ".join(glosses)}')

    final_definition = "<br>".join(english_defs)
    # print_debug(f'final definition:\n{final_definition}\n')

    return final_definition


def get_jamdict():
    if not hasattr(get_jamdict, "_instance"):
        from jamdict import Jamdict
        get_jamdict._instance = Jamdict(memory_mode = False)
    return get_jamdict._instance


def best_entries(entries, search_word, tie_break="all"):
    """
    Selects the most common Jamdict entries based on priority tags
    from kanji and reading elements.
    
    tie_break='all' -> return all top-scoring entries
    tie_break='defs' -> return the one with the most definitions among the top ones
    """
    # print_debug(f'best_entries({entries}, {search_word}, tie_break={tie_break})')

    if not entries:
        return []
    elif len(entries) == 1:
        return entries

    kana_only = KANA_RE.fullmatch(search_word) is not None
    # print_debug(f'kana only = {kana_only}')

    def score_forms(forms):
        score = 0

        for r in forms:
            # print_debug(f'check form: {r} -> {r.pri}')

            tags = r.pri
            tag_found = False

            # if 'ichi1' in tags:
            #     score += PRI_WEIGHTS['ichi1']
            #     print(f'found tag ichi1 ({PRI_WEIGHTS["ichi1"]}) -> score = {score}')
            #     tag_found = True
            # else:
            for tag in tags:
              if tag.startswith('nf'):
                  score += (50 - int(tag[2:])) * 50
                  # print_debug(f'found tag {tag[0:2]}[{tag[2:]}] ({(50 - int(tag[2:])) * 50}) -> score = {score}')
                  tag_found = True
              elif tag in PRI_WEIGHTS:
                  score += PRI_WEIGHTS[tag]
                  # print_debug(f'found tag {tag} ({PRI_WEIGHTS[tag]}) -> score = {score}')
              else:
                  score += 1

            # no 'ichi1' or 'nfXX' tags found
            # if not tag_found:
            #   for tag in tags:
            #       if tag in PRI_WEIGHTS:
            #           score += PRI_WEIGHTS[tag]
            #           print(f'found tag {tag} ({PRI_WEIGHTS[tag]}) -> score = {score}')
            #       else:
            #           score += 1

        return score


    def score_entry(e):
        # print_debug(f'score entry {e}')
        # print_debug('score = 0')

        # Check kanji elements
        kanji_score = score_forms(e.kanji_forms) if not kana_only else 0
        kana_score = score_forms(e.kana_forms)

        # print_debug(f'kanji score = {kanji_score}')
        # print_debug(f'kana score = {kana_score}')

        return kanji_score + kana_score

    # Compute scores
    scored = [(e, score_entry(e)) for e in entries]
    # print_debug("scored:")
    # print_debug('\n'.join(f'[{score}] {term}' for term, score in scored))
    max_score = max(score for _, score in scored)

    # print_debug(f'max score: {max_score}')

    if max_score == 0: return []

    top_entries = [e for e, score in scored if score == max_score]

    if tie_break == "all":
        return top_entries
    elif tie_break == "defs":
        # Among the tied ones, pick the entry with the most senses
        return [max(top_entries, key=lambda e: len(e.senses))]
    else:
        raise ValueError("tie_break must be 'all' or 'defs'")