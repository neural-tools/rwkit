"""
Generic file I/O interface wrapping all supported file types
"""

from pathlib import Path
from typing import Any, List, Union

from .common import (
    SUPPORTED_COMPRESSION_EXTS,
    SUPPORTED_FILE_EXTS,
    SUPPORTED_FILE_TYPES,
)
from .docx import read_docx, write_docx
from .json import read_json, read_jsonl, write_json, write_jsonl
from .text import read_lines, read_text, write_lines, write_text
from .yaml import read_yaml, write_yaml


def _infer_file_type_from_filename(filename: Union[str, Path]) -> str:
    """
    Infer file type from filename extension.

    Args:
        filename (Union[str, Path]): Filename to infer type from.

    Raises:
        IsADirectoryError: If `filename` is a directory.
        ValueError: If `file_type` is not supported (see SUPPORTED_FILE_TYPES).

    Returns:
        str: Inferred file type. One of 'text', 'json', 'jsonl' or 'yaml'. Note that
            'lines' is omitted in favor or 'text', since we cannot determine from
            file extension if content should be treated as a string ('text') or a list
            of strings ('lines').
    """
    # Check: `filename` cannot be a directory
    filepath = Path(filename)
    if filepath.is_dir():
        raise IsADirectoryError("Must be a file, not a directory: '%s'" % filename)

    filepath_str = filepath.name.lower()

    if filepath_str.endswith(tuple(".txt" + ext for ext in SUPPORTED_COMPRESSION_EXTS)):
        return "text"

    if filepath_str.endswith(
        tuple(".json" + ext for ext in SUPPORTED_COMPRESSION_EXTS)
    ):
        return "json"

    if filepath_str.endswith(
        tuple(".jsonl" + ext for ext in SUPPORTED_COMPRESSION_EXTS)
    ):
        return "jsonl"

    if filepath_str.endswith(
        tuple(".yaml" + ext for ext in SUPPORTED_COMPRESSION_EXTS)
        + tuple(".yml" + ext for ext in SUPPORTED_COMPRESSION_EXTS)
    ):
        return "yaml"

    if filepath_str.endswith(".docx"):
        return "docx"

    raise ValueError(
        "Unrecognized file extension: %s\nValid file extensions are %s"
        % (filepath_str, SUPPORTED_FILE_EXTS)
    )


def read(
    filename: Union[str, Path],
    file_type: str = "infer",
    **kwargs: Any,
) -> Any:
    """
    Read file and return contents.

    Args:
        filename (Union[str, Path]): File to read.
        file_type (str, optional): File type. Valid options are 'text', 'lines', 'json',
            'jsonl', 'yaml', 'docx' or 'infer'. If 'infer', it is inferred from the file
            extension. Defaults to 'infer'.
        **kwargs: Additional arguments passed to the specific read function.

    Raises:
        ValueError: If `file_type` is not supported (see SUPPORTED_FILE_TYPES).

    Returns:
        Any: Contents of the file.
    """
    if file_type == "infer":
        file_type = _infer_file_type_from_filename(filename)

        # If 'chunksize' is specified, assume `file_type` is 'lines'
        if (file_type == "text") & ("chunksize" in kwargs):
            file_type = "lines"

    if file_type == "text":
        return read_text(filename, **kwargs)

    if file_type == "lines":
        return read_lines(filename, **kwargs)

    if file_type == "json":
        return read_json(filename, **kwargs)

    if file_type == "jsonl":
        return read_jsonl(filename, **kwargs)

    if file_type == "yaml":
        return read_yaml(filename, **kwargs)

    if file_type == "docx":
        return read_docx(filename, **kwargs)

    raise ValueError(
        "Unsupported file type: %s\nValid file types are %s"
        % (file_type, SUPPORTED_FILE_TYPES)
    )


def write(
    filename: Union[str, Path],
    text_or_object: Union[Any, List[Any]],
    file_type: str = "infer",
    **kwargs: Any,
) -> None:
    """
    Write text or object to file.

    Args:
        filename (Union[str, Path]): File to write to.
        text_or_object (Union[Any, List[Any]]): Text or object to write.
        file_type (str, optional): File type. Valid options are 'text', 'lines', 'json',
            'jsonl', 'yaml', 'docx' or 'infer'. If 'infer', it is inferred from the file
            extension. Defaults to 'infer'.

    Raises:
        ValueError: If `file_type` is not supported (see SUPPORTED_FILE_TYPES).
    """
    if file_type == "infer":
        file_type = _infer_file_type_from_filename(filename)

        # If `text_or_object` is a list, assume `file_type` is 'lines'.
        if (file_type == "text") & isinstance(text_or_object, list):
            file_type = "lines"

    if file_type == "text":
        return write_text(filename, text_or_object, **kwargs)

    if file_type == "lines":
        return write_lines(filename, text_or_object, **kwargs)

    if file_type == "json":
        return write_json(filename, text_or_object, **kwargs)

    if file_type == "jsonl":
        return write_jsonl(filename, text_or_object, **kwargs)

    if file_type == "yaml":
        return write_yaml(filename, text_or_object, **kwargs)

    if file_type == "docx":
        return write_docx(filename, text_or_object, **kwargs)

    raise ValueError(
        "Unsupported file type: %s\nValid file types are %s"
        % (file_type, SUPPORTED_FILE_TYPES)
    )
