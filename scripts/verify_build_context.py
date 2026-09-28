#!/usr/bin/env python3
"""Prove Docker's actual context excludes credentials at root and in source dirs."""

from pathlib import Path
import subprocess
import tempfile
import uuid

ROOT = Path(__file__).resolve().parents[1]


def main():
    name = ".env.context-check-" + uuid.uuid4().hex
    sentinels = [ROOT / name, ROOT / "internal" / name]
    try:
        for path in sentinels:
            path.write_text("test sentinel, not a credential\n", encoding="utf-8")
        with tempfile.TemporaryDirectory(prefix="fluxgate-context-") as temp:
            subprocess.run(["docker", "build", "--file", "-", "--output",
                            f"type=local,dest={Path(temp).resolve()}", "."],
                           input="FROM scratch\nCOPY . /context/\n", text=True,
                           cwd=ROOT, check=True, timeout=120)
            context = Path(temp) / "context"
            for required in ("go.mod", "go.sum", "cmd/ingest-api/main.go",
                             "internal/store/migrations/0007_window_end_retention_index.sql"):
                if not (context / required).is_file():
                    raise RuntimeError(f"build input missing: {required}")
            for path in context.rglob("*"):
                if not path.is_file():
                    continue
                rel = path.relative_to(context).as_posix()
                allowed = rel in ("go.mod", "go.sum", "build/docker/Dockerfile")
                allowed |= rel.startswith(("cmd/", "internal/")) and rel.endswith(".go")
                allowed |= rel.startswith("internal/store/migrations/") and rel.endswith(".sql")
                if not allowed:
                    raise RuntimeError(f"unexpected build input: {rel}")
    finally:
        for path in sentinels:
            path.unlink(missing_ok=True)
    print("PASS: only allowlisted source inputs enter Docker's build context.")


if __name__ == "__main__":
    main()
