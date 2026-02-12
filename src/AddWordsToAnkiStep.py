import json
import urllib.request
from collections import OrderedDict
from typing import Any

from .Artifact import Artifact
from .PipelineStep import PipelineStep
from .ProcessingStep import ProcessingStep

ANKI_CONNECT_URL = "http://127.0.0.1:8765"

DECK_NAME = "日本語::3. 単語::Mined Vocab"
MODEL_NAME = "TangoMiner:Japanese"


class AnkiConnectError(RuntimeError):
    pass


class AddWordsToAnkiStep(PipelineStep):
    def process(self, artifact: Artifact) -> Artifact:
        export_words_to_anki(deck_name=DECK_NAME, words=artifact.data, model_name=MODEL_NAME, progress_handler=self.progress)
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

    created = ensure_model_exists(MODEL_NAME)

    # if created:
    #     print("Model created")
    # else:
    #     print("Model already exists")

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
            jp_field = note["fields"]["Expression"]["value"]
            existing_words[jp_field] = note

        progress = (i + note_fetching_batch_size) / max(len(all_note_ids), 1) * 25
        update_progress(progress, 100, "Fetching existing notes from Anki...")

    # --------------------------------------------------
    # 2.5 DELETE OBSOLETE NOTES
    # --------------------------------------------------
    desired_words = desired_words = {
        w for w, stats in words.items()
        if not stats.invalid
    }

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
        if word not in desired_words:
            continue

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
                            "fields": word_to_anki_fields(word, stats)
                        }
                    }
                })

            old_tags = set(note["tags"])
            new_tags = set(stats.tags)

            to_add = new_tags - old_tags
            to_remove = {
                t for t in old_tags
                if t not in new_tags
            }

            if to_add:
                update_actions.append({
                    "action": "addTags",
                    "params": {
                        "notes": [note_id],
                        "tags": " ".join(to_add),
                    }
                })

            if to_remove:
                update_actions.append({
                    "action": "removeTags",
                    "params": {
                        "notes": [note_id],
                        "tags": " ".join(to_remove),
                    }
                })

            if dirty or to_add or to_remove:
                total_notes_to_update += 1
        else:
            dup_ids = anki_invoke("findNotes", {
                "query": f'Japanese:\"{word}\"'
            })
            
            if dup_ids:
                continue

            notes_to_add.append({
                "deckName": deck_name,
                "modelName": model_name,
                "fields": word_to_anki_fields(word, stats),
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
            f"Updating {total_notes_to_update} notes...",
        )

    processed = len(update_actions)

    # --------------------------------------------------
    # 5. Batch add new notes
    # --------------------------------------------------
    total_notes_to_add = len(notes_to_add)

    for i in range(0, total_notes_to_add, batch_size):
        # print(json.dumps(notes_to_add[i:i + batch_size], ensure_ascii=False, indent=2))

        anki_invoke("addNotes", {
            "notes": notes_to_add[i:i + batch_size]
        })

        update_progress(
            50 + ((processed + i) / max(total, 1) * 50),
            100,
            f"Adding {total_notes_to_add} new notes...",
        )

    update_progress(
        100,
        100,
        f"{len(obsolete_note_ids)} deleted, {total_notes_to_update} updated, {len(notes_to_add)} added.",
    )


def word_to_anki_fields(word: str, stats):
    return {
        "Expression": word,
        "Reading": stats.reading,
        "Index": str(stats.index),
        "Frequency": str(int(stats.frequency)),
        "Score": str(stats.score),
        "Meaning": stats.definition,
        "Sentence": "<br><br>".join(
            s.to_html() for s in stats.sentences
        ) if stats.sentences else ""
    }


def anki_fields_differ_from_stats(note, stats) -> bool:
    note_fields = note["fields"]

    # Map stats attributes to Anki field names and whether they need str()
    stats_to_note_fields = {
        "reading": "Reading",
        "definition": "Meaning",
        "index": "Index",
        "frequency": "Frequency",
        "score": "Score",
    }

    for attr, field_name in stats_to_note_fields.items():
        stats_value = str(getattr(stats, attr, ""))
        note_value = note_fields[field_name]["value"]
        
        if stats_value != note_value:
            # print(f'different {field_name}: {stats_value} != {note_value}')
            return True

    # Handle sentence separately
    note_sentence = note_fields["Sentence"]["value"]
    stats_sentence = "<br><br>".join(
        s.to_html() for s in stats.sentences
    ) if stats.sentences else ""
    
    if stats_sentence != note_sentence:
        # print(f'sentences differ: {stats_sentence} != {note_sentence}')
        return True

    return False


def ensure_model_exists(model_name: str):
    existing_models = anki_invoke("modelNames")

    if model_name in existing_models:
        return False  # already exists

    fields = [
        "Expression",
        "Reading",
        "Index",
        "Frequency",
        "Score",
        "Meaning",
        "Sentence",
    ]

    templates = [
        {
            "Name": "Placeholder",
            "Front": """
<span id="kanji">{{Expression}}</span>
<br>
<span id="pronunciation" class="hint">?</span>

{{#Sentence}}<br><br>{{Sentence}}{{/Sentence}}

<div id="tags"></div>
<div id="console"></div>

<script>
function log(message) {
  document.getElementById('console').innerHTML += message + '<br>';
}

document.querySelector('#pronunciation').addEventListener('click', e => {
  e.preventDefault();
  e.stopPropagation();
  e.target.textContent = isKana ? '{{Expression}}' : '{{Reading}}';
  e.target.classList.remove('hint');
});

var isKana = false;

</script>

<script>
if (`{{Reading}}`.length < 1) {
  document.getElementById('pronunciation').classList.add('hidden');
}
</script>
""",
            "Back": """
<span id="kanji">{{Expression}}</span>

{{#Reading}}<br><span id="pronunciation">{{Reading}}</span>{{/Reading}}

<br>
<br>

{{Meaning}}

{{#Sentence}}<br><br>{{Sentence}}{{/Sentence}}

<hr>

<div id="definitions"></div>
<div id="see-also" class="hidden">See also:<div class="content"></div></div>

<div id="tags">{{Tags}}
<br>
<br>
{{Index}} • {{Frequency}} • {{Score}}</div>
<br>
<a href="https://jisho.org/search/{{Expression}}">Jisho</a>
<br>
<br>
<a href="intent:#Intent;package=jp.takoboto;action=jp.takoboto.SEARCH;S.q={{Expression}};end">Takoboto</a>

<div id="tags"></div>
<div id="console"></div>

<script>
function log(message) {
  document.getElementById('console').textContent += message + '<br>';
}

// log(document.querySelector('#tags').outerHTML);
</script>

<script>
// tags
function createTags(tagList) {
  var tagContainer = document.getElementById('tags');

  for (var n = 0; n < tagList.length; n++) {
    var tag = tagList[n];

    if (!tag) continue;
    
    tag = tag.toLowerCase();
    
    if (tag && tag !== 'yomichan') {
      var tagElement = document.createElement('span');
      tagElement.textContent = tag;
      tagContainer.appendChild(tagElement);
    }
  }
}

//createTags(`{{Tags}}`.split(' '));
</script>
""",
        }
    ]

    css = """
.card {
 font-family: arial, no-serif;
 font-size: 1.5em;
 text-align: center;
 color: black;
 background-color: white;
}
.card.night_mode {
  color: #eeeeee;
}

div {
  margin: 1em 0;
}

hr {
  border: 0;
  border-bottom: 1px solid lightgray;
  padding: 0;
  background: none;
}

a {
  text-decoration: none;
  color: lightblue;
}

.hidden {
  display: none;
}

.hint {
  cursor: pointer;
  text-decoration: underline dotted;
}

.highlight {
  color: lightblue;
}

#kanji {
  font-size: 2rem;
}

#pronunciation {
  font-size: 0.75em;
}

.sentence-tag {
    font-size: 0.75em;
    opacity: .8;
}

#tags {
  font-size: 0.5em;
}

#tags > * {
  display: inline-block;
  padding: 0.5em;
  background: red;
  border-radius: 2px;
  margin-right: 2em;
}

.card.night_mode #tags > * {
  background: #222222;
  color: e0e0e0;
}

#definitions > * {
  margin: 0;
}

#see-also .content > * {
  margin-right: 1em;
}
"""

    anki_invoke(
        "createModel",
        {
            "modelName": model_name,
            "inOrderFields": fields,
            "css": css,
            "cardTemplates": templates,
        },
    )

    return True