import json
import urllib.request
from collections import OrderedDict
from typing import Any

from .Artifact import Artifact
from .PipelineStep import PipelineStep
from .ProcessingStep import ProcessingStep

ANKI_CONNECT_URL = "http://127.0.0.1:8765"


class AnkiConnectError(RuntimeError):
    pass


class AddWordsToAnkiStep(PipelineStep):
    def process(self, artifact: Artifact) -> Artifact:
        export_words_to_anki(deck_name="日本語::3. 単語::Mined Vocab", words=artifact.data, model_name="jp.takoboto", progress_handler=self.progress)
        return artifact



def anki_invoke(action: str, params: dict[str, Any] | None = None) -> Any:
    payload = {
        "action": action,
        "version": 6,
        "params": params or {},
    }

    req = urllib.request.Request(
        ANKI_CONNECT_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=10) as resp:
        result = json.loads(resp.read().decode("utf-8"))

    if result.get("error"):
        raise AnkiConnectError(result["error"])

    return result["result"]


def get_model_fields(model_name: str) -> list[str]:
    return anki_invoke("modelFieldNames", {"modelName": model_name})


def export_words_to_anki(
    deck_name: str,
    words: dict,
    model_name: str,
    progress_handler=None,
):
    def update_progress(current, total, message: str):
        nonlocal progress_handler
        if progress_handler:
            progress_handler(ProcessingStep.ANKI_EXPORT, current, total, message)

    batch_size = 50
    note_fetching_batch_size = 1000
    existing_words = {}

    # --------------------------------------------------
    # 1. Fetch ALL existing note IDs for this model/deck
    # --------------------------------------------------
    # update_progress(0, 100, "Fetching existing notes from Anki...")

    all_note_ids = anki_invoke("findNotes", {
        "query": f'deck:\"{deck_name}\" note:\"{model_name}\"'
    })

    # --------------------------------------------------
    # 2. Fetch notesInfo in batches
    # --------------------------------------------------
    for i in range(0, len(all_note_ids), note_fetching_batch_size):
        batch_ids = all_note_ids[i:i + note_fetching_batch_size]

        notes = anki_invoke("notesInfo", {
            "notes": batch_ids
        })

        for note in notes:
            jp_field = note["fields"]["Japanese"]["value"]
            existing_words[jp_field] = note

        progress = (i + note_fetching_batch_size) / max(len(all_note_ids), 1) * 25
        update_progress(progress, 100, "Fetching existing notes from Anki...")

    # --------------------------------------------------
    # 2.5 DELETE OBSOLETE NOTES
    # --------------------------------------------------
    desired_words = set(words.keys())
    existing_set = set(existing_words.keys())

    obsolete_words = existing_set - desired_words
    obsolete_note_ids = [
        existing_words[word]["noteId"]
        for word in obsolete_words
    ]

    for i in range(0, len(obsolete_note_ids), batch_size):
        anki_invoke("deleteNotes", {
            "notes": obsolete_note_ids[i:i + batch_size]
        })

    # --------------------------------------------------
    # 3. Prepare add + update batches
    # --------------------------------------------------
    notes_to_add = []
    update_actions = []
    total_notes_to_update = 0

    for i, (word, stats) in enumerate(words.items(), start=1):
        new_tags = set(stats.tags)

        if word in existing_words:
            note = existing_words[word]
            note_id = note["noteId"]

            dirty = anki_fields_differ_from_stats(note, stats)

            if dirty:
                update_actions.append({
                    "action": "updateNoteFields",
                    "params": {
                        "note": {
                            "id": note_id,
                            "fields": {
                                "Japanese": word,
                                "Reading": stats.reading,
                                "Meaning": stats.definition,
                                "Position": str(stats.index),
                                "Frequency": str(int(stats.frequency)),
                                "FrequencyNormalized": str(stats.score),
                                "Sentence": '<br><br>'.join(stats.sentences) if stats.sentences else ""
                            }
                        }
                    }
                })

            old_tags = set(note["tags"])
            merged = new_tags - old_tags
            if merged:
                update_actions.append({
                    "action": "addTags",
                    "params": {
                        "notes": [note_id],
                        "tags": " ".join(merged),
                    }
                })

            if dirty or merged:
                total_notes_to_update += 1
        else:
            # dup_ids = anki_invoke("findNotes", {
            #     "query": f'Japanese:\"{word}\"'
            # })
            # if dup_ids:
            #     continue

            notes_to_add.append({
                "deckName": deck_name,
                "modelName": model_name,
                "fields": {
                    "Japanese": word,
                    "Reading": stats.reading,
                    "Meaning": stats.definition,
                    "Position": str(stats.index),
                    "Frequency": str(stats.frequency),
                    "FrequencyNormalized": str(stats.score),
                    "Sentence": '<br><br>'.join(stats.sentences) if stats.sentences else ""
                },
                "tags": list(new_tags),
            })

        update_progress(
            25 + (i / len(words) * 25),
            100,
            "Preparing actions...",
        )

    total = len(update_actions) + len(notes_to_add)
    processed = 0

    # --------------------------------------------------
    # 4. Batch update existing notes
    # --------------------------------------------------
    for i in range(0, len(update_actions), batch_size):
        anki_invoke("multi", {
            "actions": update_actions[i:i + batch_size]
        })

        update_progress(
            50 + (i / max(total, 1) * 50),
            100,
            "Updating existing notes...",
        )

    processed = len(update_actions)

    # --------------------------------------------------
    # 5. Batch add new notes
    # --------------------------------------------------
    for i in range(0, len(notes_to_add), batch_size):
        anki_invoke("addNotes", {
            "notes": notes_to_add[i:i + batch_size]
        })

        update_progress(
            50 + ((processed + i) / max(total, 1) * 50),
            100,
            "Adding new notes...",
        )

    update_progress(
        100,
        100,
        f"{len(obsolete_note_ids)} deleted, {total_notes_to_update} updated, {len(notes_to_add)} added.",
    )


def anki_fields_differ_from_stats(note, stats) -> bool:
    note_fields = note["fields"]

    # Map stats attributes to Anki field names and whether they need str()
    stats_to_note_fields = {
        "reading": "Reading",
        "definition": "Meaning",
        "index": "Position",
        "frequency": "Frequency",
        "score": "FrequencyNormalized",
    }

    for attr, field_name in stats_to_note_fields.items():
        stats_value = str(getattr(stats, attr))
        note_value = note_fields[field_name]["value"]
        
        if stats_value != note_value:
            return True

    # Handle sentence separately
    note_sentence = note_fields["Sentence"]["value"]
    stats_sentence = '<br><br>'.join(stats.sentences) if stats.sentences else ""
    
    if stats_sentence != note_sentence:
        return True

    return False
