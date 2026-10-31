"""Render a bounded, human-readable overview from the recorded run summary."""
from pathlib import Path


def render_report(summary):
    lines = ["# Tango Miner — Pipeline Report", "", f"Status: {summary.get('status', 'unavailable').upper()}"]
    duration = summary.get("duration_seconds")
    if isinstance(duration, (int, float)):
        lines.append(f"Duration: {duration:.1f}s (excluding recorded user wait)")
    if summary.get("error"):
        lines.append(f"Error: {summary['error']}")
    stages = summary.get("stages", [])
    stages = stages if isinstance(stages, list) else []
    by_name = {stage.get("name"): stage for stage in stages if isinstance(stage, dict)}
    definitions = by_name.get("AddDefinitionsStep", {})
    lines += ["", "## Candidates"]
    for key, label in (("valid_candidates", "Valid"), ("review_candidates", "Needs review"), ("invalid_candidates", "Invalid")):
        if key in definitions:
            lines.append(f"- {label}: {definitions[key]:,} (at definition stage)")
    comparison = by_name.get("CompareOutputStep", {}).get("comparison", {})
    if comparison.get("status") == "compared":
        counts = comparison.get("counts", {})
        lines.append("- CSV candidate changes: " + ", ".join(f"{key} {counts.get(key, 0):,}" for key in ("added", "removed", "changed")))
        lines.append(f"- CSV baseline: ../{comparison['baseline']}/summary.json")
        lines.append("- Changes are candidate differences, not Anki changes; check input and configuration before interpreting them.")
    elif comparison.get("status") == "no_baseline":
        lines.append("- CSV comparison: no previous completed run")
    if len(lines) == lines.index("## Candidates") + 1:
        lines.append("- Unavailable")
    lines += ["", "## Grammar"]
    grammar = by_name.get("GrammarStep", {})
    rules = grammar.get("grammar_rules")
    if isinstance(rules, dict):
        lines.append(f"- Recorded rule occurrences: {sum(row.get('occurrences', 0) for row in rules.values()):,} (rules may overlap)")
    else:
        lines.append("- " + ("Disabled" if summary.get("grammar_enabled") is False else "Unavailable"))
    anki = summary.get("anki") or {}
    lines += ["", "## Anki", f"- Status: {anki.get('status', 'unavailable')}"]
    plan = anki.get("plan")
    if isinstance(plan, dict):
        lines.append("- Planned: " + ", ".join(f"{key} {plan.get(key, 0):,}" for key in ("add", "modify", "delete")))
    lines += ["", "## Performance"]
    timed = [stage for stage in stages if isinstance(stage, dict) and isinstance(stage.get("duration_seconds"), (int, float))]
    for stage in sorted(timed, key=lambda item: item["duration_seconds"], reverse=True)[:5]:
        lines.append(f"- {stage.get('name', 'Unknown')}: {stage['duration_seconds']:.1f}s ({stage.get('status', 'unavailable')})")
    if not timed:
        lines.append("- Unavailable")
    lines += ["", "Details: [summary.json](summary.json); [stage records](steps/); [CSV changes](csv-changes.json) when available.", ""]
    return "\n".join(lines)


def write_run_report(directory, summary):
    target = Path(directory) / "report.md"
    temporary = target.with_suffix(".md.tmp")
    temporary.write_text(render_report(summary), encoding="utf-8", newline="\n")
    temporary.replace(target)
