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
    words: list,
    model_name: str,
    progress_handler=None,
):
    from collections import defaultdict

    batch_size = 50
    existing_words = {}

    # --------------------------------------------------
    # 1. Fetch ALL existing note IDs for this model/deck
    # --------------------------------------------------
    if progress_handler:
        progress_handler(ProcessingStep.ANKI_EXPORT, 0, 100, "Fetching existing notes from Anki...")

    all_note_ids = anki_invoke("findNotes", {
        "query": f'deck:\"{deck_name}\" note:\"{model_name}\"'
    })

    # --------------------------------------------------
    # 2. Fetch notesInfo in batches
    # --------------------------------------------------
    for i in range(0, len(all_note_ids), batch_size):
        batch_ids = all_note_ids[i:i + batch_size]

        notes = anki_invoke("notesInfo", {
            "notes": batch_ids
        })

        for note in notes:
            jp_field = note["fields"]["Japanese"]["value"]
            existing_words[jp_field] = note

        if progress_handler:
            progress = int((i + batch_size) / max(len(all_note_ids), 1) * 100)
            progress_handler(
                ProcessingStep.ANKI_EXPORT,
                0,
                100,
                f"Fetching existing notes from Anki... {min(progress, 100)}%",
            )

    # --------------------------------------------------
    # 3. Prepare add + update batches
    # --------------------------------------------------
    notes_to_add = []
    update_actions = []
    total_notes_to_update = 0

    for word, stats in words.items():
        new_tags = set(stats.tags)

        if word in existing_words:
            note = existing_words[word]
            note_id = note["noteId"]

            # TODO
            dirty = anki_fields_differ_from_stats(note, stats)

            if dirty:
                # ---- update fields ----
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
                                "Frequency": str(stats.frequency),
                                "FrequencyNormalized": str(stats.score),
                            }
                        }
                    }
                })

            # ---- merge tags ----
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
            # Defensive duplicate check (Anki is stricter than we are)
            dup_ids = anki_invoke("findNotes", {
                "query": f'Japanese:"{word}"'
            })

            if dup_ids:
                # Treat as existing → skip add entirely
                continue

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
                },
                "tags": list(new_tags),
            })

    total = len(update_actions) + len(notes_to_add)
    processed = 0

    # --------------------------------------------------
    # 4. Batch update existing notes (fields + tags)
    # --------------------------------------------------
    for i in range(0, len(update_actions), batch_size):
        anki_invoke("multi", {
            "actions": update_actions[i:i + batch_size]
        })

        if progress_handler:
            progress_handler(
                ProcessingStep.ANKI_EXPORT,
                i,
                total,
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

        if progress_handler:
            progress_handler(
                ProcessingStep.ANKI_EXPORT,
                processed + i,
                total,
                "Adding new notes...",
            )

    if progress_handler:
        progress_handler(ProcessingStep.ANKI_EXPORT, 100, 100, f'{total_notes_to_update} notes updated and {len(notes_to_add)} added.')


def anki_fields_differ_from_stats(note, stats) -> bool:
    note_fields = note["fields"]

    if stats.reading != note_fields["Reading"]["value"]:
        return True

    if stats.definition != note_fields["Meaning"]["value"]:
        return True

    if str(stats.index) != note_fields["Position"]["value"]:
        return True

    if str(stats.frequency) != note_fields["Frequency"]["value"]:
        return True

    if str(stats.score) != note_fields["FrequencyNormalized"]["value"]:
        return True