"""
JSON file I/O
"""

import json
import tarfile
from io import BytesIO
from pathlib import Path
from typing import Any, Generator, List, Optional, Union

from .common import open_file


def read_json(
    filename: Union[str, Path],
    mode: str = "r",
    compression: Optional[str] = "infer",
) -> Any:
    """
    Read a JSON file.

    Args:
        filename (Union[str, Path]): File to read.
        mode (str, optional): File access mode. Must start with 'r'. Defaults to 'r'.
        compression (Optional[str], optional): File compression method. Options: 'bz2',
            'gzip', 'tar', 'xz', 'zip', 'zstd', None (no compression), or 'infer'. Use
            'infer' for automatic detection based on file extension. For tar archives,
            use 'infer' with appropriate file extensions ('.tar.bz2', '.tar.gz', '.tgz',
            '.tar.xz') or use 'tar' with `mode` set to 'r:bz2', 'r:gz', or 'r:xz'.
            Defaults to 'infer'.

    Raises:
        ValueError: If `mode` does not start with 'r'.

    Returns:
        Any: A single JSON-serializable object.
    """
    # Check mode
    if not mode.startswith("r"):
        raise ValueError("Unrecognized mode: %s\nValid modes start with: 'r'" % mode)

    with open_file(filename, mode, compression) as (_, file_handle, is_binary):
        content = file_handle.read()

        if is_binary:
            content = content.decode()

        return json.loads(content)


def write_json(
    filename: Union[str, Path],
    data: Any,
    mode: str = "w",
    compression: Optional[str] = "infer",
    level: Optional[int] = None,
) -> None:
    """
    Write a JSON-serializable object to a file.

    Args:
        filename (Union[str, Path]): File to write to.
        data (Any): JSON-serializable object to write to file.
        mode (str, optional): File access mode. Must start with 'w' or 'x'. Defaults to
            'w'.
        compression (Optional[str], optional): File compression method. Options: 'bz2',
            'gzip', 'tar', 'xz', 'zip', 'zstd', None (no compression), or 'infer'. Use
            'infer' for automatic detection based on file extension. For tar archives,
            use 'infer' with appropriate file extensions ('.tar.bz2', '.tar.gz', '.tgz',
            '.tar.xz') or use 'tar' with `mode` ending in ':bz2', ':gz', or ':xz'.
            Defaults to 'infer'.
        level (Optional[int], optional): Compression level. Only used if `compression`
            is not None. Valid values depend on the compression method, typically
            ranging from 0 (no compression) to 9 (highest compression). If None, the
            default level for each compression method is used. Defaults to None.

    Raises:
        ValueError: If `mode` does not start with 'w' or 'x'.

    Note:
        A newline character is added at the end of the JSON content.
    """
    # Check mode
    valid_modes = ("w", "x")
    if not mode.startswith(valid_modes):
        raise ValueError(
            "Unrecognized mode: %s\nValid modes start with: %s" % (mode, valid_modes)
        )

    with open_file(filename, mode, compression, level) as (
        container_handle,
        file_handle,
        is_binary,
    ):
        content = json.dumps(data) + "\n"

        if is_binary:
            content = content.encode()

        # Write out
        if isinstance(file_handle, tarfile.TarInfo):
            file_handle.size = len(content)
            container_handle.addfile(file_handle, fileobj=BytesIO(content))
        else:
            file_handle.write(content)


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
        mode (str, optional): File access mode. Must start with 'r'. Defaults to 'r'.
        compression (Optional[str], optional): File compression method. Options: 'bz2',
            'gzip', 'tar', 'xz', 'zip', 'zstd', None (no compression), or 'infer'. Use
            'infer' for automatic detection based on file extension. For tar archives,
            use 'infer' with appropriate file extensions ('.tar.bz2', '.tar.gz', '.tgz',
            '.tar.xz') or use 'tar' with `mode` set to 'r:bz2', 'r:gz', or 'r:xz'.
            Defaults to 'infer'.
        chunksize (int, optional): Number of JSON-serializable objects (= lines) to read
            as a chunk. Defaults to 1.

    Raises:
        ValueError: If `mode` does not start with 'r'.
        ValueError: If `chunksize` is not 1 or greater.

    Yields:
        Generator[List[Any], Any, None]: Lists of JSON-serializable objects.
    """
    # Check mode
    if not mode.startswith("r"):
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


def read_jsonl(
    filename: Union[str, Path],
    mode: str = "r",
    compression: Optional[str] = "infer",
    chunksize: Optional[int] = None,
) -> Union[List[Any], Generator[List[Any], None, None]]:
    """
    Read JSON Lines file.

    Args:
        filename (Union[str, Path]): Path to file.
        mode (str, optional): File access mode. Must start with 'r'. Defaults to 'r'.
        compression (Optional[str], optional): File compression method. Options: 'bz2',
            'gzip', 'tar', 'xz', 'zip', 'zstd', None (no compression), or 'infer'. Use
            'infer' for automatic detection based on file extension. For tar archives,
            use 'infer' with appropriate file extensions ('.tar.bz2', '.tar.gz', '.tgz',
            '.tar.xz') or use 'tar' with `mode` set to 'r:bz2', 'r:gz', or 'r:xz'.
            Defaults to 'infer'.
        chunksize (Optional[int], optional): If None, reads all JSON-serializable
            objects (= lines) at once. If integer, reads the file in chunks of
            `chunksize`. Defaults to None.

    Raises:
        ValueError: If `mode` does not start with 'r'.
        ValueError: If `chunksize` is not None and less than 1.

    Returns:
        Union[List[Any], Generator[List[Any], None, None]]: If `chunksize` is None,
            returns a list of JSON-serializable objects. If `chunksize` is an integer,
            returns a generator that yields lists of JSON-serializable objects in chunks
            of `chunksize`.
    """
    # Check mode
    if not mode.startswith("r"):
        raise ValueError("Unrecognized mode: %s\nValid modes start with: r" % mode)

    if chunksize is None:
        with open_file(filename, mode, compression) as (_, file_handle, is_binary):
            content = file_handle.read()

            if is_binary:
                content = content.decode()

            return [json.loads(line) for line in content.rstrip("\n").split("\n")]

    return _read_jsonl_generator(filename, mode, compression, chunksize)


def write_jsonl(
    filename: Union[str, Path],
    data: Union[Any, List[Any]],
    mode: str = "w",
    compression: Optional[str] = "infer",
    level: Optional[int] = None,
) -> None:
    """
    Write JSON-serializable object(s) to a JSON Lines file.

    Args:
        filename (Union[str, Path]): File to write to.
        data (Union[Any, List[Any]]): JSON-serializable object (or a list thereof) to
            write, one per line.
        mode (str, optional): File access mode. Must start with 'w', 'x', or 'a'.
            Defaults to 'w'.
        compression (Optional[str], optional): File compression method. Options: 'bz2',
            'gzip', 'tar', 'xz', 'zip', 'zstd', None (no compression), or 'infer'. Use
            'infer' for automatic detection based on file extension. For tar archives,
            use 'infer' with appropriate file extensions ('.tar.bz2', '.tar.gz', '.tgz',
            '.tar.xz') or use 'tar' with `mode` ending in ':bz2', ':gz', or ':xz'.
            Defaults to 'infer'.
        level (Optional[int], optional): Compression level. Only used if `compression`
            is not None. Valid values depend on the compression method, typically
            ranging from 0 (no compression) to 9 (highest compression). If None, the
            default level for each compression method is used. Defaults to None.

    Raises:
        ValueError: If `mode` does not start with 'w', 'x', or 'a'.
    """
    # Check mode
    valid_modes = ("w", "x", "a")
    if not mode.startswith(valid_modes):
        raise ValueError(
            "Unrecognized mode: %s\nValid modes start with: %s" % (mode, valid_modes)
        )

    with open_file(filename, mode, compression, level) as (
        container_handle,
        file_handle,
        is_binary,
    ):
        if isinstance(data, list):
            data_serialized = [json.dumps(item) for item in data]
        else:
            data_serialized = [json.dumps(data)]
        content = "\n".join(data_serialized) + "\n"

        if is_binary:
            content = content.encode()

        # Write out
        if isinstance(file_handle, tarfile.TarInfo):
            file_handle.size = len(content)
            container_handle.addfile(file_handle, fileobj=BytesIO(content))
        else:
            file_handle.write(content)
