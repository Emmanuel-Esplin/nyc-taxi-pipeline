"""Create or complete the local .env with random secrets.

    python3 scripts/make_secrets.py          # or: make secrets

Values that are already set in .env are never changed, so this is safe to re-run; only
missing secrets are generated. The file is git-ignored and written with mode 600.
Secrets are URL-safe (letters, digits, '-' and '_'), so they can sit inside connection URLs,
SQL literals and Makefiles without escaping.
"""

from __future__ import annotations

import os
import secrets
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = ROOT / ".env"
EXAMPLE = ROOT / ".env.example"

SECRETS = [
    "POSTGRES_PASSWORD",
    "AIRFLOW_JWT_SECRET",
    "ANALYTICS_WRITER_PASSWORD",
    "METABASE_READER_PASSWORD",
    "METABASE_APP_DB_PASSWORD",
    "MB_ADMIN_PASSWORD",
]


def parse(text: str) -> dict[str, str]:
    values = {}
    for raw_line in text.splitlines():
        entry = raw_line.strip()
        if entry and not entry.startswith("#") and "=" in entry:
            key, value = entry.split("=", 1)
            values[key.strip()] = value.strip()
    return values


def main() -> None:
    created = not ENV_FILE.exists()
    text = ENV_FILE.read_text() if not created else EXAMPLE.read_text()
    existing = parse(text)

    lines = text.splitlines()
    generated = []
    for key in SECRETS:
        if existing.get(key):
            continue
        value = secrets.token_urlsafe(24)
        # Fill an empty `KEY=` line from the example if there is one, else append.
        for i, line in enumerate(lines):
            if line.strip() == f"{key}=":
                lines[i] = f"{key}={value}"
                break
        else:
            lines.append(f"{key}={value}")
        generated.append(key)

    ENV_FILE.write_text("\n".join(lines) + "\n")
    os.chmod(ENV_FILE, 0o600)
    action = "created" if created else "updated"
    if generated:
        print(f"{action} {ENV_FILE.name}; generated: {', '.join(generated)}")
    else:
        print(f"{ENV_FILE.name} already has every secret; nothing changed")


if __name__ == "__main__":
    main()
