import json
import time
import urllib.request
from collections import OrderedDict
from functools import partial
from pathlib import Path
from typing import Any

from src.Artifact import Artifact
from src.PipelineStep import PipelineStep
from src.RunConfig import AnkiConfig
from src.steps.ProcessingStep import ProcessingStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS

ANKI_CONNECT_URL = AnkiConfig().endpoint
DECK_NAME = AnkiConfig().deck
MODEL_NAME = AnkiConfig().model


class AnkiConnectError(RuntimeError):
    pass


class AddWordsToAnkiStep(PipelineStep):
    def __init__(self, config: AnkiConfig | None = None, plan_path: Path | None = None):
        self.config = config if config is not None else AnkiConfig()
        self.plan_path = Path(plan_path) if plan_path is not None else None
        self._processing_step = ProcessingStep.ANKI_EXPORT
        self.outcome = {"status": "not_started", "plan": None}


    def process(self, artifact: Artifact) -> Artifact:
        self.wait_seconds = 0.0
        invoke = partial(anki_invoke, endpoint=self.config.endpoint, timeout=self.config.timeout_seconds)
        preview = []
        self.outcome = {"status": "preparing", "plan": None}
        def record_plan(plan):
            preview.append(plan)
            add, modify, delete, model = plan
            self.outcome["plan"] = {"add": add, "modify": modify, "delete": delete, "create_model": model}
        def record_status(status):
            self.outcome["status"] = status
        def record_wait(seconds):
            self.wait_seconds += seconds
        def save_details(details):
            if self.plan_path is not None:
                self.plan_path.write_text(json.dumps(details, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
                print(f"Anki detailed plan: {self.plan_path}")
        added, updated, deleted = export_words_to_anki(
            deck_name=self.config.deck, words=artifact.data, model_name=self.config.model,
            progress_handler=self.progress, invoke=invoke, batch_size=self.config.batch_size,
            note_fetching_batch_size=self.config.note_fetch_batch_size, output_only=self.config.output_only,
            prune=self.config.prune,
            preview_handler=record_plan, status_handler=record_status, details_handler=save_details,
            wait_handler=record_wait,
        )
        
        if self.config.output_only and preview:
            to_add, to_modify, to_delete, create_model = preview[0]
            self.done(f"Changes: {to_add} to add, {to_modify} to modify, {to_delete} to delete.")
            if create_model:
                print(f"The note model {self.config.model!r} will also be created.")
            print("Anki output-only mode: no changes applied.")
        else:
            self.done(f"{added} added, {updated} updated, {deleted} deleted.")

        return artifact


def anki_invoke(action: str, params: dict[str, Any] | None = None, *, endpoint: str = ANKI_CONNECT_URL, timeout: float = 10) -> Any:
    payload = {
        "action": action,
        "version": 6,
        "params": params or {},
    }

    req = urllib.request.Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=timeout) as resp:
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
    progress_handler=lambda *a: None,
    invoke=None,
    batch_size=50,
    note_fetching_batch_size=1000,
    output_only=False,
    prune=False,
    preview_handler=None,
    status_handler=None,
    details_handler=None,
    wait_handler=None,
):
    invoke = anki_invoke if invoke is None else invoke
    existing_words = {}
    expression_note_ids = {}

    model_exists = model_name in invoke("modelNames")

    # --------------------------------------------------
    # 1. Fetch ALL existing note IDs for this model/deck
    # --------------------------------------------------
    all_note_ids = invoke("findNotes", {
        "query": f'deck:\"{deck_name}\" note:\"{model_name}\"'
    })

    # --------------------------------------------------
    # 2. Fetch notesInfo in batches
    # --------------------------------------------------
    for i in range(0, len(all_note_ids), note_fetching_batch_size):
        batch_ids = all_note_ids[i:i + note_fetching_batch_size]

        notes = invoke("notesInfo", {
            "notes": batch_ids
        })

        for note in notes:
            jp_field = note["fields"]["Expression"]["value"]
            existing_words[jp_field] = note
            expression_note_ids.setdefault(jp_field, []).append(note["noteId"])

        progress = (i + note_fetching_batch_size) / max(len(all_note_ids), 1) * 25
        progress_handler(progress, 100, "Fetching existing notes from Anki...")

    # --------------------------------------------------
    # 2.5 DELETE OBSOLETE NOTES
    # --------------------------------------------------
    # Keep existing notes for unresolved candidates, but do not add or update
    # them until a trustworthy definition is available.
    desired_words = {w for w, stats in words.items() if not stats.invalid}
    exportable_words = {w for w in desired_words if not words[w].needs_review}

    existing_set = set(existing_words.keys())

    absent_words = existing_set - desired_words
    # An opt-in grammar card may be absent in a later vocabulary-only run.
    obsolete_words = (absent_words - GRAMMAR_EXPRESSIONS - LEGACY_GRAMMAR_EXPRESSIONS.keys()) if prune else set()
    obsolete_note_ids = [
        existing_words[word]["noteId"]
        for word in obsolete_words
    ]

    total_notes_to_delete = len(obsolete_note_ids)

    # --------------------------------------------------
    # 3. Compare notes and build the preview
    # --------------------------------------------------
    additions = []
    update_details = []
    dirty_updates = set()
    total_notes_to_update = 0
    grammar_migration_conflicts = []

    for i, (word, stats) in enumerate(words.items(), start=1):
        if word not in exportable_words:
            continue

        if word in GRAMMAR_EXPRESSIONS:
            legacy = word + ' (grammar)'
            legacy_ids = expression_note_ids.get(legacy, [])
            current_ids = expression_note_ids.get(word, [])
            if legacy_ids:
                if len(legacy_ids) != 1 or current_ids:
                    grammar_migration_conflicts.append({
                        'expression': word, 'legacy_expression': legacy,
                        'legacy_note_ids': legacy_ids, 'current_note_ids': current_ids})
                    continue
                note = existing_words[legacy]
                total_notes_to_update += 1
                update_details.append({'expression': word, 'note_id': note['noteId'],
                                       'fields': {'Expression': {'before': legacy, 'after': word}},
                                       'tags_added': [],
                                       'tags_removed': ['grammar'] if 'grammar' in note['tags'] and 'grammar' not in stats.tags else []})
                continue

        # Preserve existing grammar fields and other tags, but remove the old
        # synthetic tag unless it is also a tag of a current input source.
        if word in GRAMMAR_EXPRESSIONS and word in existing_words:
            note = existing_words[word]
            if 'grammar' in note['tags'] and 'grammar' not in stats.tags:
                total_notes_to_update += 1
                update_details.append({'expression': word, 'note_id': note['noteId'],
                                       'fields': {}, 'tags_added': [], 'tags_removed': ['grammar']})
            continue

        new_tags = set(stats.tags)

        if word in existing_words:
            note = existing_words[word]
            note_id = note["noteId"]

            dirty = anki_fields_differ_from_stats(note, stats)

            old_tags = set(note["tags"])
            new_tags = set(stats.tags)

            to_add = new_tags - old_tags
            to_remove = {
                t for t in old_tags
                if t not in new_tags
            }

            if dirty or to_add or to_remove:
                if dirty:
                    dirty_updates.add(word)
                total_notes_to_update += 1
                new_fields = word_to_anki_fields(word, stats)
                changed_fields = {
                    name: {"before": note["fields"][name]["value"], "after": value}
                    for name, value in new_fields.items()
                    if dirty and name in note["fields"] and note["fields"][name]["value"] != value
                }
                update_details.append({"expression": word, "note_id": note_id,
                                       "fields": changed_fields,
                                       "tags_added": sorted(to_add), "tags_removed": sorted(to_remove)})

        else:
            dup_ids = invoke("findNotes", {
                "query": f'deck:\"{deck_name}\" note:\"{model_name}\" Expression:\"{word}\"'
            })
            
            if dup_ids:
                continue

            additions.append({"expression": word, "fields": word_to_anki_fields(word, stats),
                              "tags": sorted(new_tags)})

        progress_handler(
            25 + (i / len(words) * 25),
            100,
            "Comparing notes..."
        )

    total_notes_to_add = len(additions)
    if details_handler is not None:
        details_handler({
            "version": 2, "deck": deck_name, "model": model_name, "prune": prune,
            "create_model": not model_exists and bool(total_notes_to_add),
            "add": additions,
            "modify": update_details,
            "delete": [{"expression": word, "note_id": existing_words[word]["noteId"]}
                       for word in sorted(obsolete_words)],
            "retained_absent": [{"expression": word, "note_id": existing_words[word]["noteId"]}
                                for word in sorted(absent_words - obsolete_words)],
            "retained_existing_grammar": [{"expression": word, "note_id": existing_words[word]["noteId"]}
                                          for word in sorted((GRAMMAR_EXPRESSIONS | LEGACY_GRAMMAR_EXPRESSIONS.keys()) & existing_set)],
            "grammar_migration_conflicts": grammar_migration_conflicts,
        })
    if preview_handler is not None:
        preview_handler((total_notes_to_add, total_notes_to_update, total_notes_to_delete,
                         not model_exists and bool(total_notes_to_add)))
    if output_only and preview_handler is not None:
        if status_handler: status_handler("preview_only")
        return 0, 0, 0
    print(f"Anki plan: {total_notes_to_add} to add, {total_notes_to_update} to modify, {total_notes_to_delete} to delete.")
    if not model_exists and total_notes_to_add:
        print(f"The note model {model_name!r} will also be created.")
    if output_only:
        print("Anki output-only mode: no changes applied.")
        return 0, 0, 0
    if not (total_notes_to_add or total_notes_to_update or total_notes_to_delete):
        if status_handler: status_handler("no_changes")
        return 0, 0, 0
    started_waiting = time.perf_counter()
    try:
        try:
            approved = input("Apply these Anki changes? [y/N] ").strip().lower() == "y"
        except EOFError:
            approved = False
    finally:
        if wait_handler:
            wait_handler(time.perf_counter() - started_waiting)
    if not approved:
        print("Anki changes cancelled.")
        if status_handler: status_handler("cancelled")
        return 0, 0, 0
    if status_handler: status_handler("applying")
    notes_to_add, update_actions = build_mutation_actions(
        deck_name, model_name, additions, update_details, dirty_updates, words)
    if not model_exists and total_notes_to_add:
        ensure_model_exists(model_name, invoke=invoke)
    for i in range(0, total_notes_to_delete, batch_size):
        invoke("deleteNotes", {"notes": obsolete_note_ids[i:i + batch_size]})

    total = len(update_actions) + len(notes_to_add)
    processed = 0

    # --------------------------------------------------
    # 4. Batch update existing notes
    # --------------------------------------------------
    for i in range(0, len(update_actions), batch_size):
        invoke("multi", {
            "actions": update_actions[i:i + batch_size]
        })

        progress_handler(
            50 + (i / max(total, 1) * 50),
            100,
            f"Updating {total_notes_to_update} notes..."
        )

    processed = len(update_actions)

    # --------------------------------------------------
    # 5. Batch add new notes
    # --------------------------------------------------
    total_notes_to_add = len(notes_to_add)

    for i in range(0, total_notes_to_add, batch_size):
        # print(json.dumps(notes_to_add[i:i + batch_size], ensure_ascii=False, indent=2))

        invoke("addNotes", {
            "notes": notes_to_add[i:i + batch_size]
        })

        progress_handler(
            50 + ((processed + i) / max(total, 1) * 50),
            100,
            f"Adding {total_notes_to_add} new notes..."
        )

    if status_handler: status_handler("applied")
    return total_notes_to_add, total_notes_to_update, total_notes_to_delete


def build_mutation_actions(deck_name, model_name, additions, update_details, dirty_updates, words):
    notes_to_add = [{"deckName": deck_name, "modelName": model_name,
                     "fields": item["fields"], "tags": item["tags"]}
                    for item in additions]
    update_actions = []
    for item in update_details:
        note_id = item["note_id"]
        if item["expression"] in dirty_updates:
            update_actions.append({"action": "updateNoteFields", "params": {"note": {
                "id": note_id, "fields": word_to_anki_fields(item["expression"], words[item["expression"]])}}})
        elif 'Expression' in item['fields']:
            update_actions.append({"action": "updateNoteFields", "params": {"note": {
                "id": note_id, "fields": {"Expression": item['fields']['Expression']['after']}}}})
        for action, tags in (("addTags", item["tags_added"]), ("removeTags", item["tags_removed"])):
            if tags:
                update_actions.append({"action": action, "params": {"notes": [note_id], "tags": " ".join(tags)}})
    return notes_to_add, update_actions


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


def ensure_model_exists(model_name: str, invoke=None):
    invoke = anki_invoke if invoke is None else invoke
    existing_models = invoke("modelNames")

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

    invoke(
        "createModel",
        {
            "modelName": model_name,
            "inOrderFields": fields,
            "css": css,
            "cardTemplates": templates,
        },
    )

    return True
