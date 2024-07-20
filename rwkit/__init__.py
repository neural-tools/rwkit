"""
rwkit
"""

from .docx import read_docx, write_docx
from .generic import read, write
from .json import read_json, read_jsonl, write_json, write_jsonl
from .text import read_text, write_text
from .yaml import read_yaml, write_yaml

__all__ = [
    "read",
    "read_docx",
    "read_json",
    "read_jsonl",
    "read_text",
    "read_yaml",
    "write",
    "write_docx",
    "write_json",
    "write_jsonl",
    "write_text",
    "write_yaml",
]
