from __future__ import annotations

import argparse
import os
import subprocess
import sys
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


ROOT = Path(__file__).resolve().parent
PROTOTYPE_DIR = ROOT / "prototype"
BUILD_SCRIPT = ROOT / "technical_implementation" / "build_evolved_prototype.py"
DATA_FILE = PROTOTYPE_DIR / "data" / "prototype_data.json"


def build_if_needed(force: bool) -> None:
    if force or not DATA_FILE.exists():
        subprocess.run([sys.executable, str(BUILD_SCRIPT)], check=True, cwd=ROOT)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Serve the evolved CACEIS prototype locally.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8000, type=int)
    parser.add_argument("--rebuild", action="store_true", help="Regenerate prototype data before serving.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    build_if_needed(force=args.rebuild)
    os.chdir(PROTOTYPE_DIR)
    server = ThreadingHTTPServer((args.host, args.port), SimpleHTTPRequestHandler)
    print(f"Serving prototype on http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
