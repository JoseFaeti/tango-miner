"""Save a complete comparison against the preceding completed run."""
from src.PipelineStep import PipelineStep
from src.RunComparison import build_change_report, completed_runs, load_run, write_change_report


class CompareOutputStep(PipelineStep):
    def __init__(self, output_path):
        self.csv_path = output_path
        self.path = output_path.parent / "csv-changes.json"
        self.comparison = None

    def process(self, artifact):
        current = self.csv_path.parent
        # Final summary timestamps order completions even when revisions differ
        # within the same minute. The current run is still marked running.
        candidates = [p for p in completed_runs(current.parent) if p != current]
        previous = max(candidates, key=lambda p: ((p / "summary.json").stat().st_mtime_ns, p.name), default=None)
        before = load_run(previous) if previous else None
        after = load_run(current, require_completed=False)
        result = build_change_report(before, after)
        write_change_report(self.path, result)
        self.comparison = {key: result[key] for key in ("status", "baseline", "counts")}
        self.progress(1, 1, "CSV comparison written." if before else "No previous completed run to compare.")
        return artifact
