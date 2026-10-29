import time
from typing import Iterable, Any, List

from src.PipelineStep import PipelineStep, ProgressEvent


class Pipeline:
    def __init__(self, steps: Iterable[PipelineStep], on_progress=None, cache=None, on_stage_record=None):
        self.steps: List[PipelineStep] = list(steps)
        self.on_progress = on_progress
        self.cache = cache
        self.on_stage_record = on_stage_record
        self.stage_records = []
        self._current_record = None

        for step in self.steps:
            step._progress_handler = self._handle_progress


    def run(self, initial_input: Any) -> Any:
        self.start_time = time.perf_counter()

        data = initial_input
        tmpdir = data.tmpdir

        self.stage_records = []
        self.wait_seconds = 0.0
        try:
            for index, step in enumerate(self.steps, 1):
                step.start_time = time.perf_counter()
                record = {"name": type(step).__name__, "status": "running"}
                if isinstance(data.data, (list, dict, tuple)):
                    record["input_count"] = len(data.data)
                self.stage_records.append(record)
                self._current_record = record
                if self.on_stage_record:
                    self.on_stage_record(index, step, record)
                try:
                    data = self.cache.process(step, data) if self.cache is not None else step.process(data)
                    data.tmpdir = tmpdir
                    record["status"] = "completed"
                    if hasattr(data.data, "__len__"):
                        record["output_count"] = len(data.data)
                    if isinstance(data.data, list) and all(
                        isinstance(item, tuple) and len(item) == 2 and isinstance(item[1], list)
                        for item in data.data
                    ):
                        record["output_sentence_count"] = sum(len(item[1]) for item in data.data)
                    if isinstance(data.data, dict) and type(step).__name__ == "AddDefinitionsStep":
                        record["valid_candidates"] = sum(not stats.invalid and not stats.needs_review for stats in data.data.values())
                        record["review_candidates"] = sum(stats.needs_review and not stats.invalid for stats in data.data.values())
                        record["invalid_candidates"] = sum(stats.invalid for stats in data.data.values())
                except BaseException as exc:
                    record["status"] = "failed"
                    record["error"] = f"{type(exc).__name__}: {exc}"
                    raise
                finally:
                    wall_seconds = time.perf_counter() - step.start_time
                    wait_seconds = getattr(step, "wait_seconds", 0.0)
                    self.wait_seconds += wait_seconds
                    record["duration_seconds"] = max(0.0, wall_seconds - wait_seconds)
                    if hasattr(step, "cache_stats"):
                        record["cache"] = dict(step.cache_stats)
                    if self.cache is not None and type(step).__name__ == "MergeTokensStep" and self.cache.stats is not None:
                        record["cache"] = dict(self.cache.stats)
                    if hasattr(step, "outcome"):
                        record["anki"] = dict(step.outcome)
                    if type(step).__name__ == "GrammarStep" and record["status"] == "completed":
                        record["grammar_rules"] = {
                            rule.id: {"expression": rule.expression,
                                      "occurrences": step.occurrences[rule.id].count,
                                      "examples": len(step.occurrences[rule.id].examples)}
                            for rule in step.rules
                        }
                    if self.on_stage_record:
                        self.on_stage_record(index, step, record)
                    self._current_record = None
        finally:
            self.end_time = time.perf_counter()
            self.duration = max(0.0, self.end_time - self.start_time - self.wait_seconds)

        return data


    def _handle_progress(self, event: ProgressEvent):
        if self._current_record is not None and event.current >= event.total and event.message:
            self._current_record["message"] = event.message
        if self.on_progress:
            self.on_progress(event.step, event.current, event.total, duration=event.duration, additional_text=event.message)
