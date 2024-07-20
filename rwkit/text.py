"""
Text file I/O
"""

import tarfile
from io import BytesIO
from pathlib import Path
from typing import Any, Generator, Iterator, List, Optional, Union

from .common import open_file


def _read_text_generator(
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


def read_text(
    filename: Union[str, Path],
    mode: str = "r",
    compression: Optional[str] = "infer",
    lines: bool = False,
    chunksize: Optional[int] = None,
) -> Union[str, List[str], Iterator[List[str]]]:
    """
    Read text file and return content as a string or list of strings.

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
        lines (bool, optional): If True, returns lines as list of strings. If False,
            returns a single string. Defaults to False.
        chunksize (Optional[int], optional): If None, reads all lines at once. If
            integer, reads the file in chunks of `chunksize` lines. Defaults to None.

    Raises:
        ValueError: If `mode` does not start with 'r'.
        ValueError: If lines is False and `chunksize` is not None.
        ValueError: If `chunksize` is not 1 or greater.

    Returns:
        Union[str, List[str], Iterator[List[str]]]: Content of file. If `lines` is True
            and `chunksize` is None, returns whole file as list of strings (1 string
            per line). If `lines` is True and `chunksize` is an integer, a generator is
            returned that yields lists of strings in chunks of `chunksize`. Finally, if
            `lines` is False, `chunksize` is ignored and the whole file is returned as a
            single string.
    """
    # Checks
    if not mode[0].startswith("r"):
        raise ValueError("Unrecognized mode: %s\nValid modes start with: 'r'" % mode)

    if (not lines) and (chunksize is not None):
        raise ValueError("chunksize must be None if lines is False")

    if (chunksize is None) or (not lines):
        # Process whole file at once
        with open_file(filename, mode, compression) as (_, file_handle, is_binary):
            content = file_handle.read()

            if is_binary:
                content = content.decode()

            if lines:
                # Split content into lines
                return content.rstrip("\n").split("\n")

            # Return content as single string
            return content

    # Process file in chunks
    return _read_text_generator(filename, mode, compression, chunksize)


def write_text(
    filename: Union[str, Path],
    text: Union[str, List[str]],
    mode: str = "w",
    compression: Optional[str] = "infer",
    level: Optional[int] = None,
    lines: bool = False,
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
        lines (bool, optional): If True, writes lines as list of strings. If False,
            writes a single string. Defaults to False.

    Raises:
        TypeError: If `text` is not a string or list of strings.
        ValueError: If `mode` does not start with 'w', 'x', or 'a'.
    """
    # Checks
    if isinstance(text, str):
        content = text
        if lines:
            # Add trailing newline
            content += "\n"
    elif isinstance(text, list):
        non_string_elements = set(
            [type(t).__name__ for t in lines if not isinstance(t, str)]
        )
        if non_string_elements:
            raise TypeError(
                "text must be a string or list of strings, got list with %s"
                % ", ".join(non_string_elements)
            )

        # List of strings are written as lines
        content = "\n".join(text) + "\n"
    else:
        raise TypeError("text must be a string or list of strings")

    valid_modes = ("w", "x", "a")
    if mode[0] not in valid_modes:
        raise ValueError(
            "Unrecognized mode: %s\nValid modes start with: %s" % (mode, valid_modes)
        )

    with open_file(filename, mode, compression, level) as (
        container_handle,
        file_handle,
        is_content_binary,
    ):
        # Write out
        if is_content_binary:
            content = content.encode()

        if isinstance(file_handle, tarfile.TarInfo):
            file_handle.size = len(content)
            container_handle.addfile(file_handle, fileobj=BytesIO(content))
        else:
            file_handle.write(content)