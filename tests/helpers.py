from pathlib import Path

from src.WordStats import WordStats


def make_stats(
    *,
    index=1,
    frequency=1,
    score=0.0,
    reading="よみ",
    definition="definition",
    tags=None,
    sentences=None,
    lemma="言葉",
    pos=("名詞", "普通名詞", "一般", "*"),
    invalid=False,
):
    return WordStats(
        index=index,
        frequency=frequency,
        score=score,
        reading=reading,
        definition=definition,
        tags=set(tags or []),
        sentences=list(sentences or []),
        lemma=lemma,
        pos=pos,
        invalid=invalid,
    )


def fixture_path(name: str) -> Path:
    return Path(__file__).parent / "fixtures" / name
