"""
JSON file I/O
"""

import json
import tarfile
from io import BytesIO
from pathlib import Path
from typing import Any, Generator, List, Optional, Union

from .common import open_file


def _read_jsonl_generator(
    filename: Union[str, Path],
    mode: str = "r",
    compression: Optional[str] = "infer",
    chunksize: int = 1,
) -> Generator[List[Any], Any, None]:
    """
    Generator that reads a JSON Lines file in chunks.

    Args:
        filename (Union[str, Path]): File to read.
        mode (str, optional): File access mode. Defaults to 'r'.
        compression (Optional[str], optional): File compression. Valid options are
            'bz2', 'gzip', 'tar', 'xz', 'zip', 'zstd', None (= no compression) or
            'infer'. For 'tar.bz2', 'tar.gz', 'tgz' or 'tar.xz', use
            `compression='infer'` and a `filename` ending in '.tar.bz2', '.tar.gz',
            '.tgz' or '.tar.xz', respectively. Alternatively, use `compression='tar'`
            and `mode` 'r:bz2', 'r:gz' or 'r:xz'. Defaults to 'infer'.
        chunksize (int, optional): Number of JSON-serializable objects (= lines) to read
            as a chunk. Defaults to 1.

    Raises:
        ValueError: If `mode` does not start with 'r'.
        ValueError: If `chunksize` is not 1 or greater.

    Yields:
        Generator[List[Any], Any, None]: Lists of JSON-serializable objects.
    """
    # Check mode
    if not mode[0].startswith("r"):
        raise ValueError("Unrecognized mode: %s\nValid modes start with: r" % mode)

    # Check chunksize
    if chunksize < 1:
        raise ValueError("chunksize must be 1 or greater")

    with open_file(filename, mode, compression) as (_, file_handle, is_content_binary):
        chunk: List[Any] = []
        counter = 0
        for line in file_handle:
            if is_content_binary:
                line = line.decode()

            chunk.append(json.loads(line))
            counter += 1

            # Once `chunksize` is reached, yield `chunk`
            if counter == chunksize:
                yield chunk

                # Empty list, reset counter
                chunk = []
                counter = 0

        # If `chunk` contains items after all lines have been read, yield it
        if counter > 0:
            yield chunk


def read_json(
    filename: Union[str, Path],
    mode: str = "r",
    compression: Optional[str] = "infer",
    lines: bool = False,
    chunksize: Optional[int] = None,
) -> Any:
    """
    Read a JSON file.

    Args:
        filename (Union[str, Path]): File to read.
        mode (str, optional): File access mode. Defaults to 'r'.
        compression (Optional[str], optional): File compression. Valid options are
            'bz2', 'gzip', 'tar', 'xz', 'zip', 'zstd', None (= no compression) or
            'infer'. For 'tar.bz2', 'tar.gz', 'tgz' or 'tar.xz', use
            `compression='infer'` and a `filename` ending in '.tar.bz2', '.tar.gz',
            '.tgz' or '.tar.xz', respectively. Alternatively, use `compression='tar'`
            and `mode` 'r:bz2', 'r:gz' or 'r:xz'. Defaults to 'infer'.
        lines (bool, optional): If True, returns a list of JSON-serializable objects
            (1 per line). Defaults to False.
        chunksize (Optional[int], optional): If None, reads all lines at once. If
            integer, reads the file in chunks of `chunksize` lines. Only valid if
            `lines` is True. Defaults to None.

    Raises:
        ValueError: If `chunksize` is not 1 or greater.
        ValueError: If `mode` does not start with 'r'.

    Returns:
        Any: If `lines` is False, returns a single JSON-serializable object. If `lines`
            is True, returns a list of JSON-serializable objects (1 per line).
            Additionally, if `chunksize` is None, returns all JSON-serializable objects
            at once. If `chunksize` is an integer, returns a generator that yields lists
            of JSON-serializable objects in chunks of `chunksize`.
    """
    # Check mode
    if not mode[0].startswith("r"):
        raise ValueError("Unrecognized mode: %s\nValid modes start with: r" % mode)

    if chunksize is None:
        with open_file(filename, mode, compression) as (_, file_handle, is_binary):
            content = file_handle.read()

            if is_binary:
                content = content.decode()

            if lines:
                return [json.loads(line) for line in content.rstrip("\n").split("\n")]
            return json.loads(content)
    else:
        if not lines:
            raise ValueError("Specifying chunksize is only valid when lines=True")
        return _read_jsonl_generator(filename, mode, compression, chunksize)


def write_json(
    filename: Union[str, Path],
    data: Any,
    mode: str = "w",
    compression: Optional[str] = "infer",
    level: Optional[int] = None,
    lines: bool = False,
) -> None:
    """
    Write a JSON-serializable object to a file.

    Args:
        filename (Union[str, Path]): File to write to.
        data (Any): JSON-serializable object to write to file.
        mode (str, optional): File access mode. Valid options are 'w' or 'x'.
            Defaults to 'w'.
        compression (Optional[str], optional): File compression. Valid options are
            'bz2', 'gzip', 'tar', 'xz', 'zip', 'zstd', None (= no compression) or
            'infer'. For 'tar.bz2', 'tar.gz', 'tgz' or 'tar.xz', use
            `compression='infer'` and a `filename` ending in '.tar.bz2', '.tar.gz',
            '.tgz' or '.tar.xz', respectively. Alternatively, use `compression='tar'`
            and `mode` ending in ':bz2', ':gz' or ':xz'. Defaults to 'infer'.
        level (Optional[int], optional): Compression level. Only in effect if
            `compression` is not None. If `level` is None, each compression method's
            default will be used. Defaults to None.
        lines (bool, optional): If False, write a single JSON-serializable object. If
            True, write 1 JSON-serializable object per line. Defaults to False.

    Raises:
        ValueError: If `mode` does not start with 'w' or 'x'.
    """
    # Check mode
    valid_modes = ("w", "x")
    if mode[0] not in valid_modes:
        raise ValueError(
            "Unrecognized mode: %s\nValid modes start with: %s" % (mode, valid_modes)
        )

    with open_file(filename, mode, compression, level) as (
        container_handle,
        file_handle,
        is_binary,
    ):
        if lines:
            if isinstance(data, list):
                data_serialized = [json.dumps(item) for item in data]
            else:
                data_serialized = [json.dumps(data)]
            content = "\n".join(data_serialized) + "\n"
        else:
            content = json.dumps(data) + "\n"

        if is_binary:
            content = content.encode()

        # Write out
        if isinstance(file_handle, tarfile.TarInfo):
            file_handle.size = len(content)
            container_handle.addfile(file_handle, fileobj=BytesIO(content))
        else:
            file_handle.write(content)


def read_jsonl(
    filename: Union[str, Path],
    mode: str = "r",
    compression: Optional[str] = "infer",
    chunksize: Optional[int] = None,
) -> List[Any]:
    """
    Read JSON Lines file.

    Args:
        filename (Union[str, Path]): Path to file.
        mode (str, optional): File access mode. Defaults to 'r'.
        compression (Optional[str], optional): File compression. Valid options are
            'bz2', 'gzip', 'tar', 'xz', 'zip', 'zstd', None (= no compression) or
            'infer'. For 'tar.bz2', 'tar.gz', 'tgz' or 'tar.xz', use
            `compression='infer'` and a `filename` ending in '.tar.bz2', '.tar.gz',
            '.tgz' or '.tar.xz', respectively. Alternatively, use `compression='tar'`
            and `mode` 'r:bz2', 'r:gz' or 'r:xz'. Defaults to 'infer'.
        chunksize (Optional[int], optional): If None, reads all JSON-serializable
            objects (= lines) at once. If integer, reads the file in chunks of
            `chunksize`. Defaults to None.

    Returns:
        List[Any]: If `chunksize` is None, returns a list of JSON-serializable objects.
            If `chunksize` is an integer, returns a generator that yields lists of
            JSON-serializable objects in chunks of `chunksize`.
    """
    return read_json(filename, mode, compression, lines=True, chunksize=chunksize)


def write_jsonl(
    filename: Union[str, Path],
    data: Any,
    mode: str = "w",
    compression: Optional[str] = "infer",
    level: Optional[int] = None,
) -> None:
    """
    Write JSON-serializable object(s) to a JSON Lines file.

    Args:
        filename (Union[str, Path]): File to write to.
        data (Any): List of JSON-serializable objects to write.
        mode (str, optional): File access mode. Defaults to 'w'.
        compression (Optional[str], optional): File compression. Valid options are
            'bz2', 'gzip', 'tar', 'xz', 'zip', 'zstd', None (= no compression) or
            'infer'. For 'tar.bz2', 'tar.gz', 'tgz' or 'tar.xz', use
            `compression='infer'` and a `filename` ending in '.tar.bz2', '.tar.gz',
            '.tgz' or '.tar.xz', respectively. Alternatively, use `compression='tar'`
            and `mode` ending in ':bz2', ':gz' or ':xz'. Defaults to 'infer'.
        level (Optional[int], optional): Compression level. Only in effect if
            `compression` is not None. If `level` is None, each compression method's
            default will be used. Defaults to None.
    """
    write_json(filename, data, mode, compression, level, lines=True)
