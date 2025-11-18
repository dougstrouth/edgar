"""
Deprecated: tests moved to tests/test_json_parser.py

This file remains as a convenience runner if executed directly.
"""
import sys
from pathlib import Path

if __name__ == "__main__":
    # Run the relocated pytest file
    project_root = Path(__file__).resolve().parent
    target = project_root / "tests" / "test_json_parser.py"
    try:
        import pytest  # type: ignore
        sys.exit(pytest.main([str(target), "-q"]))
    except Exception as e:
        print(f"Failed to run tests: {e}")
        sys.exit(1)
