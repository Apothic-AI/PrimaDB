#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def run(*args: str, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=str(cwd or ROOT),
        text=True,
        check=True,
        capture_output=True,
    )


def main() -> None:
    wheel_dir = Path(tempfile.mkdtemp(prefix="primadb-python-wheel-"))
    consumer_dir = Path(tempfile.mkdtemp(prefix="primadb-python-consumer-"))
    try:
        run(sys.executable, "-m", "pip", "wheel", str(ROOT), "-w", str(wheel_dir))
        venv_dir = consumer_dir / ".venv"
        run(sys.executable, "-m", "venv", str(venv_dir))
        python = venv_dir / "bin" / "python"
        wheel = next(wheel_dir.glob("primadb_python-*.whl"))
        run(str(python), "-m", "pip", "install", str(wheel))

        smoke = consumer_dir / "smoke.py"
        smoke.write_text(
            "\n".join(
                [
                    "from primadb import Primadb",
                    "",
                    'db = Primadb("python-pack-check")',
                    'db.chain("notes").field("items").set({"title": "pack check", "body": "wheel install"})',
                    'entries = db.chain("notes").field("items").query({"filters": [{"kind": "eq", "path": "title", "value": "pack check"}]})',
                    'print({"replica": db.replica_id(), "count": len(entries), "title": entries[0]["value"]["title"]})',
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        result = run(str(python), str(smoke), cwd=consumer_dir)
        print(
            json.dumps(
                {
                    "wheel": wheel.name,
                    "consumerResult": result.stdout.strip(),
                    "python_package_pack_check_confirmed": True,
                },
                indent=2,
            )
        )
    finally:
        shutil.rmtree(wheel_dir, ignore_errors=True)
        shutil.rmtree(consumer_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
