"""Check the configured JMdict against the tested dictionary snapshot."""
import argparse
import hashlib
from pathlib import Path
import sys
import tomllib

ROOT = Path(__file__).resolve().parent.parent
TESTED_DATE = "2026-10-31"
TESTED_SHA256 = "3214e9b77728d3d0e67a8bf5609d25b3bc8922e0103f1c5538fb74b078e81f41"


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
