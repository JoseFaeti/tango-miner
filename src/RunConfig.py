from dataclasses import asdict, dataclass, fields, replace
import json
import math
from pathlib import Path
import tomllib
from urllib.parse import urlsplit


@dataclass(frozen=True)
class InputConfig:
    path: str | None = None
    recursive: bool = False
    encoding: str = "utf-8"
    extensions: tuple[str, ...] = (".tsv", ".txt", ".csv", ".pdf", ".xml", ".html", ".srt")


@dataclass(frozen=True)
class TokenizerConfig:
    config_path: str = "sudachi.config.json"
    dictionary: str = "full"


@dataclass(frozen=True)
class DictionaryConfig:
    jmdict_path: str = "~/JMdict_e.xml"


@dataclass(frozen=True)
class MergeConfig:
    max_lookahead: int = 10


@dataclass(frozen=True)
class FilterConfig:
    min_frequency: int = 4
    keep_percent: int = 98
    min_frequency_per_source: float = 1.1


@dataclass(frozen=True)
class ScoringConfig:
    frequency_weight: float = 0.75
    position_weight: float = 0.25
    position_exponent: float = 1.5
    tag_saturation: int = 10
    minimum_tag_factor: float = 0.1
    max_score: float = 1000.0


@dataclass(frozen=True)
class SentencesConfig:
    max_sentences: int = 3
    ideal_length: int = 25
    length_divisor: float = 15.0
    unknown_word_penalty: float = 0.9
    too_hard_word_penalty: float = 0.9
    difficulty_weight: float = 1.5
    short_sentence_penalty: float = 0.5
    min_word_count: int = 6
    variance_divisor: float = 100000.0


@dataclass(frozen=True)
class OutputConfig:
    path: str | None = None


@dataclass(frozen=True)
class AnkiConfig:
    enabled: bool = False
    output_only: bool = True
    prune: bool = False
    endpoint: str = "http://127.0.0.1:8765"
    deck: str = "日本語::3. 単語::Mined Vocab"
    model: str = "TangoMiner:Japanese"
    timeout_seconds: float = 10.0
    batch_size: int = 50
    note_fetch_batch_size: int = 1000


@dataclass(frozen=True)
class DebugConfig:
    enabled: bool = False


@dataclass(frozen=True)
class ProfilingConfig:
    enabled: bool = False


@dataclass(frozen=True)
class RunConfig:
    config_version: int = 1
    input: InputConfig = InputConfig()
    tokenizer: TokenizerConfig = TokenizerConfig()
    merge: MergeConfig = MergeConfig()
    dictionary: DictionaryConfig = DictionaryConfig()
    filter: FilterConfig = FilterConfig()
    scoring: ScoringConfig = ScoringConfig()
    sentences: SentencesConfig = SentencesConfig()
    output: OutputConfig = OutputConfig()
    debug: DebugConfig = DebugConfig()
    profiling: ProfilingConfig = ProfilingConfig()
    anki: AnkiConfig = AnkiConfig()

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=True, indent=2)


def resolve_config(path=None, overrides=None) -> RunConfig:
    config = RunConfig()
    if path is not None:
        with Path(path).open("rb") as file:
            data = tomllib.load(file)
        unknown = set(data) - {f.name for f in fields(config)}
        if unknown:
            raise ValueError(f"Unknown configuration sections: {', '.join(sorted(unknown))}")
        version = data.get("config_version", 1)
        if type(version) is not int or version != 1:
            raise ValueError(f"Unsupported config_version: {version!r}")
        for section in ("input", "tokenizer", "merge", "dictionary", "filter", "scoring", "sentences", "output", "debug", "profiling", "anki"):
            values = data.get(section, {})
            if not isinstance(values, dict):
                raise ValueError(f"[{section}] must be a table")
            config = replace(config, **{section: _update_section(section, getattr(config, section), values)})
    for section, values in (overrides or {}).items():
        config = replace(config, **{section: _update_section(section, getattr(config, section), values)})
    anki = config.anki
    try:
        endpoint = urlsplit(anki.endpoint)
        valid_endpoint = (endpoint.scheme in ("http", "https") and bool(endpoint.hostname)
                          and endpoint.username is None and endpoint.password is None
                          and endpoint.port != 0)
    except ValueError:
        valid_endpoint = False
    if not valid_endpoint:
        raise ValueError("anki.endpoint must be an HTTP(S) URL with a valid host and port")
    if not anki.deck.strip() or not anki.model.strip():
        raise ValueError("anki.deck and anki.model must not be empty")
    if not math.isfinite(anki.timeout_seconds) or anki.timeout_seconds <= 0:
        raise ValueError("anki.timeout_seconds must be finite and positive")
    if anki.batch_size < 1 or anki.note_fetch_batch_size < 1:
        raise ValueError("anki batch sizes must be positive")
    if not config.tokenizer.config_path.strip() or not config.tokenizer.dictionary.strip():
        raise ValueError("tokenizer.config_path and tokenizer.dictionary must not be empty")
    if not config.dictionary.jmdict_path.strip():
        raise ValueError("dictionary.jmdict_path must not be empty")
    if config.merge.max_lookahead < 2:
        raise ValueError("merge.max_lookahead must be at least 2")
    if config.filter.min_frequency < 1:
        raise ValueError("filter.min_frequency must be at least 1")
    if not 1 <= config.filter.keep_percent <= 100:
        raise ValueError("filter.keep_percent must be between 1 and 100")
    if not math.isfinite(config.filter.min_frequency_per_source) or config.filter.min_frequency_per_source < 0:
        raise ValueError("filter.min_frequency_per_source must be finite and nonnegative")
    scoring = config.scoring
    for name in ("frequency_weight", "position_weight", "position_exponent", "minimum_tag_factor", "max_score"):
        if not math.isfinite(getattr(scoring, name)):
            raise ValueError(f"scoring.{name} must be finite")
    if scoring.frequency_weight < 0 or scoring.position_weight < 0 or scoring.frequency_weight + scoring.position_weight <= 0:
        raise ValueError("scoring weights must be nonnegative and at least one must be positive")
    if scoring.position_exponent <= 0 or scoring.max_score <= 0:
        raise ValueError("scoring.position_exponent and scoring.max_score must be positive")
    if scoring.tag_saturation < 2:
        raise ValueError("scoring.tag_saturation must be at least 2")
    if not 0 <= scoring.minimum_tag_factor <= 1:
        raise ValueError("scoring.minimum_tag_factor must be between 0 and 1")
    sentences = config.sentences
    if sentences.max_sentences < 1 or sentences.ideal_length < 1 or sentences.min_word_count < 0:
        raise ValueError("sentences.max_sentences and ideal_length must be positive; min_word_count must be nonnegative")
    for name in ("length_divisor", "variance_divisor"):
        value = getattr(sentences, name)
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"sentences.{name} must be finite and positive")
    for name in ("unknown_word_penalty", "too_hard_word_penalty", "difficulty_weight", "short_sentence_penalty"):
        value = getattr(sentences, name)
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"sentences.{name} must be finite and nonnegative")
    if not config.input.extensions or any(not ext.startswith(".") or len(ext) < 2 for ext in config.input.extensions):
        raise ValueError("input.extensions must contain extensions beginning with a dot")
    if not config.input.encoding:
        raise ValueError("input.encoding must not be empty")
    return config


def _update_section(name, section, values):
    expected = {f.name for f in fields(section)}
    unknown = set(values) - expected
    if unknown:
        raise ValueError(f"Unknown [{name}] settings: {', '.join(sorted(unknown))}")
    types = {"input": {"path": (str, type(None)), "recursive": (bool,), "encoding": (str,), "extensions": (list, tuple)},
             "tokenizer": {"config_path": (str,), "dictionary": (str,)},
             "merge": {"max_lookahead": (int,)},
             "dictionary": {"jmdict_path": (str,)},
             "filter": {"min_frequency": (int,), "keep_percent": (int,), "min_frequency_per_source": (int, float)}, "output": {"path": (str, type(None))},
             "scoring": {"frequency_weight": (int, float), "position_weight": (int, float),
                         "position_exponent": (int, float), "tag_saturation": (int,),
                         "minimum_tag_factor": (int, float), "max_score": (int, float)},
             "sentences": {"max_sentences": (int,), "ideal_length": (int,),
                           "length_divisor": (int, float), "unknown_word_penalty": (int, float),
                           "too_hard_word_penalty": (int, float), "difficulty_weight": (int, float),
                           "short_sentence_penalty": (int, float), "min_word_count": (int,),
                           "variance_divisor": (int, float)},
             "debug": {"enabled": (bool,)}, "profiling": {"enabled": (bool,)}, "anki": {"enabled": (bool,), "output_only": (bool,), "prune": (bool,), "endpoint": (str,), "deck": (str,), "model": (str,),
                      "timeout_seconds": (int, float), "batch_size": (int,), "note_fetch_batch_size": (int,)}}
    for key, value in values.items():
        if type(value) not in types[name][key]:
            raise ValueError(f"Invalid type for {name}.{key}")
    if name == "input" and "extensions" in values:
        if any(type(ext) is not str for ext in values["extensions"]):
            raise ValueError("Invalid type for input.extensions")
        values = {**values, "extensions": tuple(ext.lower() for ext in values["extensions"])}
    return replace(section, **values)
