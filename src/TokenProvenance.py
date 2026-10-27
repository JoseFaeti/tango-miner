from dataclasses import dataclass


@dataclass(frozen=True)
class TokenComponent:
    """The original token evidence used to form a candidate."""

    surface: str
    lemma: str
    base_form: str
    reading: str
    pos: tuple[str, ...]


def token_components(token: dict) -> tuple[TokenComponent, ...]:
    """Return immutable raw components, also for an unmerged token."""
    if "components" in token:
        return token["components"]
    return (TokenComponent(
        surface=token["surface"], lemma=token["lemma"],
        base_form=token["base_form"], reading=token["reading"],
        pos=tuple(token["pos"]),
    ),)
