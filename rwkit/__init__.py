"""
rwkit
"""

from .json import read_json, read_jsonl, write_json, write_jsonl
from .text import read_lines, read_text, write_lines, write_text
from .yaml import read_yaml, write_yaml

__all__ = [
    "read_json",
    "read_jsonl",
    "read_lines",
    "read_text",
    "read_yaml",
    "write_json",
    "write_jsonl",
    "write_lines",
    "write_text",
    "write_yaml",
]
