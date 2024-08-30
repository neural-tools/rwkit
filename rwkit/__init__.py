"""
rwkit
"""

from importlib.metadata import PackageNotFoundError, version

from .io_json import read_json, read_jsonl, write_json, write_jsonl
from .io_text import read_lines, read_text, write_lines, write_text
from .io_yaml import read_yaml, write_yaml

try:
    __version__ = version("rwkit")
except PackageNotFoundError:
    # Package is not installed
    __version__ = "unknown"

__all__ = [
    "read_text",
    "write_text",
    "read_lines",
    "write_lines",
    "read_json",
    "write_json",
    "read_jsonl",
    "write_jsonl",
    "read_yaml",
    "write_yaml",
]
