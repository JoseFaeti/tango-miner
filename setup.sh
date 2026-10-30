#!/bin/sh
set -eu

cd "$(dirname "$0")"
PYTHON=${PYTHON:-python3}
VENV_DIR=${VENV_DIR:-.venv}

if ! command -v "$PYTHON" >/dev/null 2>&1; then
    echo "Python executable not found: $PYTHON" >&2
    exit 1
fi
"$PYTHON" -c 'import sys; sys.exit("Python 3.11 or newer is required") if sys.version_info < (3, 11) else None'
if [ ! -f "$VENV_DIR/bin/activate" ]; then
    "$PYTHON" -m venv "$VENV_DIR"
fi
"$VENV_DIR/bin/python" -m pip install -r requirements.txt
cat <<EOF_MESSAGE
Setup complete. Activate the environment with: . $VENV_DIR/bin/activate
Obtain JMdict_e.xml separately and set [dictionary] jmdict_path in config.toml.
EOF_MESSAGE
