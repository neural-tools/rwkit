"""
Test common functions
"""

import bz2
import gzip
import itertools
import lzma
import tarfile
import unittest
import zipfile
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory

import zstandard

from rwkit.common import open_file


class TestCommon(unittest.TestCase):
    """TestCommon"""

    def test_open_file(self):
        """test_open_file"""

        add_file_extension_list = [True, False]
        mode_list = ("r", "w", "x", "a")
        compression_list = (
            None,
            "bz2",
            "gzip",
            "tar",
            "tar.bz2",
            "tar.gz",
            "tgz",
            "tar.xz",
            "xz",
            "zip",
            "zstd",
            "?",
        )
        infer_list = [True, False]
        level_list = [None, 1, 2, 3, 4, 5, 6, 7, 8, 9]

        txts_expected = ["These", "are words", "of a sentence"]
        content = "\n".join(txts_expected) + "\n"
        content_binary = content.encode()

        for add_file_extension, mode, compression, infer, level in itertools.product(
            add_file_extension_list,
            mode_list,
            compression_list,
            infer_list,
            level_list,
        ):
            with TemporaryDirectory() as tmpdir:
                # Opening a directory raises IsADirectoryError
                with self.assertRaises(IsADirectoryError):
                    with open_file(
                        filename=tmpdir,
                        mode=mode,
                        compression=compression,
                        level=level,
                    ) as _:
                        pass

                filepath = Path(tmpdir) / "file"

                if compression == "?":
                    if mode == "r":
                        # In read mode, existence of file is checked before validation
                        # of compression (see below), hence, skip this case
                        continue

                    # In all other modes, unknown compression must raise ValueError
                    with self.assertRaises(ValueError):
                        with open_file(
                            filename=filepath,
                            mode=mode,
                            compression="?",
                            level=level,
                        ) as _:
                            pass
                    continue

                if mode == "r":
                    # File does not exist yet, read mode must raise FileNotFoundError
                    with self.assertRaises(FileNotFoundError):
                        with open_file(
                            filename=filepath,
                            mode=mode,
                            compression=compression,
                            level=level,
                        ) as _:
                            pass

                    # Create file now
                    if compression is None:
                        with open(filepath, mode="w") as handle:
                            handle.write(content)
                    elif compression == "bz2":
                        if add_file_extension:
                            filepath = filepath.with_suffix(".bz2")
                        kwargs = {"compresslevel": level} if level else {}
                        with bz2.open(filepath, mode="wb", **kwargs) as handle:
                            handle.write(content_binary)
                    elif compression == "gzip":
                        if add_file_extension:
                            filepath = filepath.with_suffix(".gz")
                        kwargs = {"compresslevel": level} if level else {}
                        with gzip.open(filepath, mode="wb", **kwargs) as handle:
                            handle.write(content_binary)
                    elif compression == "xz":
                        if add_file_extension:
                            filepath = filepath.with_suffix(".xz")
                        kwargs = {"preset": level} if level else {}
                        with lzma.open(
                            filepath, format=lzma.FORMAT_XZ, mode="wb", **kwargs
                        ) as handle:
                            handle.write(content_binary)
                    elif compression == "zip":
                        if add_file_extension:
                            filepath = filepath.with_suffix(".zip")
                        kwargs = {"compresslevel": level} if level else {}
                        with zipfile.ZipFile(
                            filepath, mode="w", **kwargs
                        ) as container_handle:
                            with container_handle.open("data", mode="w") as file_handle:
                                file_handle.write(content_binary)
                    elif compression.startswith("tar") | (compression == "tgz"):
                        if add_file_extension:
                            filepath = filepath.with_suffix("." + compression)

                        tar_mode = "w"
                        kwargs = {}
                        if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                            if "." in compression:
                                tar_mode += ":" + compression.split(".")[1]
                            else:
                                tar_mode += ":gz"

                            if level:
                                if tar_mode.endswith(":xz"):
                                    kwargs["preset"] = level
                                else:
                                    kwargs["compresslevel"] = level

                        with tarfile.open(
                            filepath, mode=tar_mode, **kwargs
                        ) as container_handle:
                            tar_info = tarfile.TarInfo(name="data")
                            tar_info.size = len(content_binary)
                            container_handle.addfile(
                                tar_info, fileobj=BytesIO(content_binary)
                            )
                    elif compression == "zstd":
                        if add_file_extension:
                            filepath = filepath.with_suffix(".zst")
                        kwargs = (
                            {"cctx": zstandard.ZstdCompressor(level=level)}
                            if level
                            else {}
                        )
                        with zstandard.open(filepath, mode="w", **kwargs) as handle:
                            handle.write(content)
                    else:
                        raise NotImplementedError(compression)

                    # Read file contents
                    if infer & ((compression is None) | add_file_extension):
                        with open_file(
                            filename=filepath,
                            mode=mode,
                            compression="infer",
                            level=level,
                        ) as _:
                            pass
                    else:
                        if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                            if "." in compression:
                                mode += ":" + compression.split(".")[1]
                            else:
                                mode += ":gz"

                            with open_file(
                                filename=filepath,
                                mode=mode,
                                compression="tar",
                                level=level,
                            ) as _:
                                pass
                        else:
                            with open_file(
                                filename=filepath,
                                mode=mode,
                                compression=compression,
                                level=level,
                            ) as _:
                                pass
                elif mode in ("w", "x"):
                    if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                        if "." in compression:
                            mode += ":" + compression.split(".")[1]
                        else:
                            mode += ":gz"

                        with open_file(
                            filename=filepath,
                            mode=mode,
                            compression="tar",
                            level=level,
                        ) as _:
                            pass
                    else:
                        with open_file(
                            filename=filepath,
                            mode=mode,
                            compression=compression,
                            level=level,
                        ) as _:
                            pass
                elif mode == "a":
                    if compression in ("tar", "zip"):
                        with self.assertRaises(ValueError):
                            with open_file(
                                filename=filepath,
                                mode=mode,
                                compression=compression,
                                level=level,
                            ) as _:
                                pass
                else:
                    raise NotImplementedError(mode)
