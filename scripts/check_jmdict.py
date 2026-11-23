"""Check the configured JMdict against the tested dictionary snapshot."""
import argparse
import hashlib
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TESTED_DATE = "2026-11-23"
TESTED_SHA256 = "d66a8ac5e9e363368d86a2f8957accb3da10cc5df5b459c5b15647041f2e7d7b"


def check_dictionary():
    config_path = ROOT / "config.toml"
    try:
        with config_path.open("rb") as config_file:
            config = tomllib.load(config_file)
        path = Path(config.get("dictionary", {}).get("jmdict_path", "~/JMdict_e.xml")).expanduser()
        if not path.is_absolute():
            path = ROOT / path
        with path.open("rb") as dictionary:
            digest = hashlib.file_digest(dictionary, "sha256").hexdigest()
    except (OSError, ValueError, TypeError) as exc:
        return False, f"JMdict baseline check unavailable: {exc}"
    if digest != TESTED_SHA256:
        return False, (f"JMdict differs from tested {TESTED_DATE} baseline: "
                       f"SHA-256 {digest}; expected {TESTED_SHA256}.")
    return True, f"JMdict matches the tested {TESTED_DATE} baseline (SHA-256 {digest})."


def main():
    argparse.ArgumentParser(description=__doc__).parse_args()
    matches, message = check_dictionary()
    print(message)
    return 0 if matches else 1


if __name__ == "__main__":
    sys.exit(main())
