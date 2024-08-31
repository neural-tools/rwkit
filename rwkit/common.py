"""
Common functions
"""

import bz2
import gzip
import lzma
import tarfile
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Dict, Iterator, Optional, Tuple, Union

try:
    import zstandard

    _HAVE_ZSTD = True
except ImportError:
    _HAVE_ZSTD = False

SUPPORTED_COMPRESSION_TYPES = (
    "bz2",
    "gzip",
    "tar",
    "xz",
    "zip",
    "zstd",
)

ContainerType = Optional[Union[object, "zstandard.ZstdCompressor"]]


@contextmanager
def open_file(
    filename: Union[str, Path],
    mode: str,
    compression: Optional[str] = None,
    level: Optional[int] = None,
) -> Iterator[Tuple[ContainerType, IO, bool]]:
    """
    Open a file with optional compression as a context manager.

    Args:
        filename (Union[str, Path]): File to open for reading or writing.
        mode (str): File access mode. For 'tar' and 'zip' compression, append mode ('a')
            is not supported.
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

    Yields:
        Iterator[Tuple[ContainerType, IO, bool]]: A tuple containing:
            - ContainerType: The container handle (e.g., TarFile, ZipFile) if
              applicable, else None.
            - IO: The file handle for reading or writing.
            - bool: A flag indicating whether the file content is binary (True) or text
              (False).

    Raises:
        IsADirectoryError: If `filename` is a directory.
        FileNotFoundError: If `mode` is 'r' and the file does not exist.
        ValueError: If `compression` is not supported or if 'tar' or 'zip' compression
            is used with append mode.
        NotImplementedError: If `compression` is not implemented.
        tarfile.ReadError: If `mode` is 'r' and file is not a valid tarfile.
        ValueError: If `mode` is 'r' and the tar or zip archive does not contain exactly
            1 file.
        zipfile.BadZipFile: If `mode` is 'r' and file is not a valid zipfile.
        ModuleNotFoundError: If `compression` is 'zstd' and the zstandard module is not
            installed.

    Note:
        This function returns a context manager and is best used with a 'with'
        statement. The ContainerType in the returned tuple is defined as:
        ContainerType = Optional[Union[object, "zstandard.ZstdCompressor"]]
    """
    # Check: `filename` cannot be a directory
    filepath = Path(filename)
    if filepath.is_dir():
        raise IsADirectoryError("Must be a file, not a directory: '%s'" % filename)

    # Check if file exists when opening in read mode
    if mode.startswith("r") and not filepath.is_file():
        raise FileNotFoundError("No such file: '%s'" % filename)

    # Check compression
    valid_compression_types = (None, "infer") + SUPPORTED_COMPRESSION_TYPES
    if compression not in valid_compression_types:
        raise ValueError(
            "Unsupported compression: %s\nValid compression types are %s"
            % (compression, valid_compression_types)
        )

    # Infer compression based on filename extension
    if compression == "infer":
        filepath_str = filepath.name.lower()

        if filepath_str.endswith(".tar"):
            compression = "tar"
            level = None
        elif filepath_str.endswith(".tar.bz2"):
            compression = "tar"
            if mode in ("r", "w", "x"):
                mode += ":bz2"
        elif filepath_str.endswith(".tar.gz") or filepath_str.endswith(".tgz"):
            compression = "tar"
            if mode in ("r", "w", "x"):
                mode += ":gz"
        elif filepath_str.endswith(".tar.xz"):
            compression = "tar"
            if mode in ("r", "w", "x"):
                mode += ":xz"
        elif filepath_str.endswith(".bz2"):
            compression = "bz2"
        elif filepath_str.endswith(".gz"):
            compression = "gzip"
        elif filepath_str.endswith(".xz"):
            compression = "xz"
        elif filepath_str.endswith(".zip"):
            compression = "zip"
        elif filepath_str.endswith(".zst"):
            compression = "zstd"
        else:
            # Compression cannot be inferred, assume no compression
            compression = None
    elif (compression == "tar") & (len(mode) == 1):
        level = None

    if mode.startswith("r"):
        # Reading mode does not require a compression level
        level = None

    kwargs: Dict[str, Any] = {"mode": mode}
    if compression is None:
        with open(filename, **kwargs) as file_handle:
            yield None, file_handle, False
    elif compression == "tar":
        if level:
            if mode.endswith(":xz"):
                kwargs["preset"] = level
            else:
                kwargs["compresslevel"] = level

        container_handle = tarfile.open(filename, **kwargs)
        try:
            if mode.startswith("r"):
                if not tarfile.is_tarfile(filename):
                    raise tarfile.ReadError(filename)

                file_list = container_handle.getnames()
                if len(file_list) != 1:
                    raise ValueError("tar archive must contain exactly 1 file")
                file_handle = container_handle.extractfile(file_list[0])
            elif mode.startswith(("w", "x")):
                file_handle = tarfile.TarInfo(name="data")
            elif mode.startswith("a"):
                raise ValueError("tar does not support append mode")

            yield container_handle, file_handle, True
        finally:
            container_handle.close()
    elif compression == "bz2":
        if level:
            kwargs["compresslevel"] = level

        with bz2.BZ2File(filename, **kwargs) as file_handle:
            yield None, file_handle, True
    elif compression == "gzip":
        if level:
            kwargs["compresslevel"] = level

        with gzip.GzipFile(filename, **kwargs) as file_handle:
            yield None, file_handle, True
    elif compression == "xz":
        kwargs["format"] = lzma.FORMAT_XZ
        if level:
            kwargs["preset"] = level

        with lzma.LZMAFile(filename, **kwargs) as file_handle:
            yield None, file_handle, True
    elif compression == "zip":
        if level:
            kwargs["compresslevel"] = level

        container_handle = zipfile.ZipFile(filename, **kwargs)
        try:
            if mode == "r":
                if not zipfile.is_zipfile(filename):
                    raise zipfile.BadZipFile(filename)

                file_list = container_handle.namelist()
                if len(file_list) != 1:
                    raise ValueError("zip archive must contain exactly 1 file")

                file_in_container = file_list[0]
            elif mode in ("w", "x"):
                file_in_container = "data"
                # files in zip archive must be opened in write mode
                mode = "w"
            elif mode == "a":
                raise ValueError("zip does not support append mode")

            with container_handle.open(file_in_container, mode=mode) as file_handle:
                yield container_handle, file_handle, True
        finally:
            container_handle.close()
    elif compression == "zstd":
        if not _HAVE_ZSTD:
            raise ModuleNotFoundError(
                "No module named 'zstandard'. Install with $ pip install zstandard"
            )
        if level:
            kwargs["cctx"] = zstandard.ZstdCompressor(level=level)

        with zstandard.open(filename, **kwargs) as file_handle:
            yield None, file_handle, False
    else:
        raise NotImplementedError(compression)
