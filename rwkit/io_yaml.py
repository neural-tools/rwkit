"""
YAML file I/O
"""

import tarfile
from io import BytesIO
from pathlib import Path
from typing import Any, Optional, Union

from .common import open_file

try:
    import yaml

    _HAVE_YAML = True
except ImportError:
    _HAVE_YAML = False


def read_yaml(
    filename: Union[str, Path],
    mode: str = "r",
    compression: Optional[str] = "infer",
) -> Any:
    """
    Read a YAML file.

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
        ModuleNotFoundError: If package 'pyyaml' is not installed.
        ValueError: If `mode` does not start with 'r'.

    Returns:
        Any: The parsed YAML content as a Python object.

    Note:
        This function uses yaml.safe_load() internally, which may raise yaml.YAMLError
        if there's an issue parsing the YAML content.
    """
    if not _HAVE_YAML:
        raise ModuleNotFoundError(
            "No module named 'yaml'. Install with: $ pip install pyyaml"
        )

    # Check mode
    if not mode.startswith("r"):
        raise ValueError("Unrecognized mode: %s\nValid modes start with: r" % mode)

    with open_file(filename, mode, compression) as (_, file_handle, is_content_binary):
        content = file_handle.read()

        if is_content_binary:
            content = content.decode()

        return yaml.safe_load(content)


def write_yaml(
    filename: Union[str, Path],
    data: Any,
    mode: str = "w",
    compression: Optional[str] = "infer",
    level: Optional[int] = None,
) -> None:
    """
    Write a YAML-serializable object to a YAML file.

    Args:
        filename (Union[str, Path]): File to write to.
        data (Any): YAML-serializable object to write.
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
        ModuleNotFoundError: If package 'pyyaml' is not installed.
        ValueError: If `mode` does not start with 'w' or 'x'.

    Note:
        The YAML content is dumped with `sort_keys=False`, preserving the original order
        of keys in dictionaries.
    """
    if not _HAVE_YAML:
        raise ModuleNotFoundError(
            "No module named 'yaml'. Install with: $ pip install pyyaml"
        )

    # Check mode
    valid_modes = ("w", "x")
    if not mode.startswith(valid_modes):
        raise ValueError(
            "Unrecognized mode: %s\nValid modes start with: %s" % (mode, valid_modes)
        )

    with open_file(filename, mode, compression, level) as (
        container_handle,
        file_handle,
        is_content_binary,
    ):
        content = yaml.dump(data, sort_keys=False)

        if is_content_binary:
            content = content.encode()

        # Write out
        if isinstance(file_handle, tarfile.TarInfo):
            file_handle.size = len(content)
            container_handle.addfile(file_handle, fileobj=BytesIO(content))
        else:
            file_handle.write(content)
