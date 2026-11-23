#!/usr/bin/env python3
"""Run plain assert-style contract tests without requiring pytest."""

import importlib.util
from pathlib import Path
import sys


def main() -> int:
    paths = [Path(value) for value in sys.argv[1:]]
    if not paths:
        raise SystemExit("pass one or more contract test files")

    count = 0
    for path in paths:
        spec = importlib.util.spec_from_file_location(path.stem, path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"cannot load {path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        for name in sorted(dir(module)):
            candidate = getattr(module, name)
            if name.startswith("test_") and callable(candidate):
                candidate()
                count += 1

    print(f"{count} direct contract tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
