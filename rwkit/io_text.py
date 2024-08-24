"""
Text file I/O
"""

import tarfile
from io import BytesIO
from pathlib import Path
from typing import Any, Generator, Iterator, List, Optional, Union

from .common import open_file


def read_text(
    filename: Union[str, Path],
    mode: str = "r",
    compression: Optional[str] = "infer",
) -> str:
    """
    Read text file and return content as a single string.

    Args:
        filename (Union[str, Path]): File to read.
        mode (str, optional): File access mode. Valid modes start with 'r'. Defaults
            to 'r'.
        compression (Optional[str], optional): File compression. Valid options are
            'bz2', 'gzip', 'tar', 'xz', 'zip', 'zstd', None (= no compression) or
            'infer'. For 'tar.bz2', 'tar.gz', 'tgz' or 'tar.xz', use
            `compression='infer'` and a `filename` ending in '.tar.bz2', '.tar.gz',
            '.tgz' or '.tar.xz', respectively. Alternatively, use `compression='tar'`
            and `mode` 'r:bz2', 'r:gz' or 'r:xz'. Defaults to 'infer'.
        chunksize (Optional[int], optional): If None, reads all lines at once. If
            integer, reads the file in chunks of `chunksize` lines. Defaults to None.

    Raises:
        ValueError: If `mode` does not start with 'r'.

    Returns:
        str: Content of file.
    """
    # Check mode
    if not mode[0].startswith("r"):
        raise ValueError("Unrecognized mode: %s\nValid modes start with: 'r'" % mode)

    with open_file(filename, mode, compression) as (_, file_handle, is_binary):
        content = file_handle.read()

        if is_binary:
            return content.decode()

        return content


def write_text(
    filename: Union[str, Path],
    text: str,
    mode: str = "w",
    compression: Optional[str] = "infer",
    level: Optional[int] = None,
) -> None:
    """
    Write text to a file.

    Args:
        filename (Union[str, Path]): File to write to.
        text (str): String to write.
        mode (str, optional): File access mode. Valid modes start with 'w', 'x', or 'a'.
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

    Raises:
        TypeError: If `text` is not a string.
        ValueError: If `mode` does not start with 'w', 'x', or 'a'.
    """
    # Checks
    if not isinstance(text, str):
        raise TypeError("text must be a string. Use write_lines() for list of strings.")

    valid_modes = ("w", "x", "a")
    if mode[0] not in valid_modes:
        raise ValueError(
            f"Unrecognized mode: {mode}\nValid modes start with: {valid_modes}"
        )

    with open_file(filename, mode, compression, level) as (
        container_handle,
        file_handle,
        is_binary,
    ):
        if is_binary:
            text = text.encode()

        if isinstance(file_handle, tarfile.TarInfo):
            file_handle.size = len(text)
            container_handle.addfile(file_handle, fileobj=BytesIO(text))
        else:
            file_handle.write(text)


def _read_lines_generator(
    filename: Union[str, Path],
    mode: str = "r",
    compression: Optional[str] = "infer",
    chunksize: int = 1,
) -> Generator[List[str], Any, None]:
    """
    Generator function for reading a text file line-by-line in chunks.

    Args:
        filename (Union[str, Path]): File to read.
        mode (str, optional): File access mode. Valid modes start with 'r'. Defaults
            to 'r'.
        compression (Optional[str], optional): File compression. Valid options are
            'bz2', 'gzip', 'tar', 'xz', 'zip', 'zstd', None (= no compression) or
            'infer'. For 'tar.bz2', 'tar.gz', 'tgz' or 'tar.xz', use
            `compression='infer'` and a `filename` ending in '.tar.bz2', '.tar.gz',
            '.tgz' or '.tar.xz', respectively. Alternatively, use `compression='tar'`
            and `mode` 'r:bz2', 'r:gz' or 'r:xz'. Defaults to 'infer'.
        chunksize (int, optional): The number of lines to read at once. Defaults to 1.

    Raises:
        ValueError: If `mode` does not start with 'r'.
        ValueError: If `chunksize` is not 1 or greater.

    Yields:
        Generator[List[str], Any, None]: A list of lines from the file.
    """
    # Checks
    if not mode[0].startswith("r"):
        raise ValueError("Unrecognized mode: %s\nValid modes start with: 'r'" % mode)

    if not (chunksize >= 1):
        raise ValueError("chunksize must be 1 or greater")

    with open_file(filename, mode, compression) as (_, file_handle, is_binary):
        chunk: List[str] = []
        counter = 0
        for line in file_handle:
            if is_binary:
                line = line.decode()

            chunk.append(line.rstrip("\n"))
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


def read_lines(
    filename: Union[str, Path],
    mode: str = "r",
    compression: Optional[str] = "infer",
    chunksize: Optional[int] = None,
) -> Union[List[str], Iterator[List[str]]]:
    """
    Read text file and return lines as list of strings.

    Args:
        filename (Union[str, Path]): File to read.
        mode (str, optional): File access mode. Defaults to 'r'.
        compression (Optional[str], optional): File compression. Valid options are
            'bz2', 'gzip', 'tar', 'xz', 'zip', 'zstd', None (= no compression) or
            'infer'. For 'tar.bz2', 'tar.gz', 'tgz' or 'tar.xz', use
            `compression='infer'` and a `filename` ending in '.tar.bz2', '.tar.gz',
            '.tgz' or '.tar.xz', respectively. Alternatively, use `compression='tar'`
            and `mode` 'r:bz2', 'r:gz' or 'r:xz'. Defaults to 'infer'.
        chunksize (Optional[int], optional): If None, reads all lines at once. If
            integer, reads the file in chunks of `chunksize` lines. Defaults to None.

    Returns:
        List[str]: If `chunksize` is None, returns a list of strings. If `chunksize` is
            an integer, returns a generator that yields lists of strings in chunks of
            `chunksize`.
    """
    if chunksize is None:
        return read_text(filename, mode, compression).rstrip("\n").split("\n")

    return _read_lines_generator(filename, mode, compression, chunksize)


def write_lines(
    filename: Union[str, Path],
    lines: Union[str, List[str]],
    mode: str = "w",
    compression: Optional[str] = "infer",
    level: Optional[int] = None,
) -> None:
    """
    Write string or list of strings to a file with trailing newlines.

    Args:
        filename (Union[str, Path]): File to write to.
        lines (Union[str, List[str]]): String or list of strings to write.
        mode (str, optional): File access mode. Valid modes start with 'w', 'x', or 'a'.
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

    Raises:
        TypeError: If `lines` is not a string or list of strings.
        TypeError: If `lines` is a list with non-string elements.
    """
    # Checks
    if isinstance(lines, str):
        lines = [lines]
    elif isinstance(lines, list):
        non_string_elements = set(
            [type(t).__name__ for t in lines if not isinstance(t, str)]
        )
        if non_string_elements:
            raise TypeError(
                "lines must be a string or list of strings, got list with %s"
                % ", ".join(non_string_elements)
            )
    else:
        raise TypeError(
            "lines must be a string or list of strings, got %s" % type(lines).__name__
        )

    content = "\n".join(lines) + "\n"
    write_text(filename, content, mode, compression, level)
