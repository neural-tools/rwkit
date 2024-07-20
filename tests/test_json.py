"""
Test JSON file I/O
"""

import bz2
import gzip
import itertools
import json
import lzma
import tarfile
import unittest
import zipfile
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory

import zstandard

from rwkit.json import read_json, read_jsonl, write_json, write_jsonl


class TestJson(unittest.TestCase):
    """TestJson"""

    def test_read_json(self):
        """test_read_json"""

        object_expected_list = [
            {"A": "a", "B": 1, "C": 0.1, "D": True, "E": False, "F": None},
            [{"A": "a", "B": 1, "C": 0.1, "D": True, "E": False, "F": None}],
            0,
            0.1,
            True,
            False,
            None,
            "This is a test",
            ["This is a test"],
            ["This is a test", "This is another test"],
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            [0, 1, 3, "A", "b", "c", True, False, None],
        ]

        add_file_extension_list = [True, False]
        infer_list = [True, False]
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
        )
        lines_list = [True, False]

        for (
            object_expected,
            add_file_extension,
            compression,
            infer,
            lines,
        ) in itertools.product(
            object_expected_list,
            add_file_extension_list,
            compression_list,
            infer_list,
            lines_list,
        ):
            with TemporaryDirectory() as tmpdir:
                filepath = Path(tmpdir) / "file"

                if lines:
                    if isinstance(object_expected, list):
                        object_expected_serialized = [
                            json.dumps(item) for item in object_expected
                        ]
                    else:
                        object_expected_serialized = [json.dumps(object_expected)]

                    content = "\n".join(object_expected_serialized) + "\n"
                    content_bytes = content.encode()
                else:
                    content = json.dumps(object_expected)
                    content_bytes = content.encode()

                # Write to file
                if compression is None:
                    with open(filepath, mode="w") as handle:
                        handle.write(content)
                elif compression == "bz2":
                    if add_file_extension:
                        filepath = filepath.with_suffix(".bz2")
                    with bz2.open(filepath, mode="wb") as handle:
                        handle.write(content_bytes)
                elif compression == "gzip":
                    if add_file_extension:
                        filepath = filepath.with_suffix(".gz")
                    with gzip.open(filepath, mode="wb") as handle:
                        handle.write(content_bytes)
                elif compression == "xz":
                    if add_file_extension:
                        filepath = filepath.with_suffix(".xz")
                    with lzma.open(
                        filepath, format=lzma.FORMAT_XZ, mode="wb"
                    ) as handle:
                        handle.write(content_bytes)
                elif compression == "zip":
                    if add_file_extension:
                        filepath = filepath.with_suffix(".zip")
                    with zipfile.ZipFile(filepath, mode="w") as container_handle:
                        with container_handle.open("data", mode="w") as file_handle:
                            file_handle.write(content_bytes)
                elif compression in ("tar", "tar.bz2", "tar.gz", "tgz", "tar.xz"):
                    if add_file_extension:
                        filepath = filepath.with_suffix("." + compression)

                    tar_mode = "w"
                    if compression in ("tar.bz2", "tar.gz", "tar.xz"):
                        tar_mode += ":" + compression.split(".")[1]
                    elif compression == "tgz":
                        tar_mode += ":gz"

                    with tarfile.open(filepath, mode=tar_mode) as container_handle:
                        tar_info = tarfile.TarInfo(name="data")
                        tar_info.size = len(content_bytes)
                        container_handle.addfile(
                            tar_info,
                            fileobj=BytesIO(content_bytes),
                        )
                elif compression == "zstd":
                    if add_file_extension:
                        filepath = filepath.with_suffix(".zst")
                    with zstandard.open(filepath, mode="w") as handle:
                        handle.write(content)
                else:
                    raise NotImplementedError(compression)

                # Read file contents
                if infer & ((compression is None) | add_file_extension):
                    object_observed = read_json(
                        filename=filepath,
                        mode="r",
                        compression="infer",
                        lines=lines,
                        chunksize=None,
                    )
                else:
                    if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                        if "." in compression:
                            mode = "r:" + compression.split(".")[1]
                        else:
                            mode = "r:gz"

                        object_observed = read_json(
                            filename=filepath,
                            mode=mode,
                            compression="tar",
                            lines=lines,
                            chunksize=None,
                        )
                    else:
                        object_observed = read_json(
                            filename=filepath,
                            mode="r",
                            compression=compression,
                            lines=lines,
                            chunksize=None,
                        )

                if lines:
                    if isinstance(object_observed, list) & (
                        not isinstance(object_expected, list)
                    ):
                        object_observed = object_observed[0]

                self.assertEqual(
                    object_expected,
                    object_observed,
                    "read_json() failed.\n"
                    "Parameters:\n"
                    f"  compression: {compression}\n"
                    f"  lines:       {lines}\n"
                    f"Expected: '{object_expected}'\n"
                    f"Observed: '{object_observed}'",
                )

                # Read file contents with chunksize
                # chunksize = 0 should raise ValueError
                with self.assertRaises(ValueError):
                    next(
                        read_json(
                            filename=filepath,
                            mode="r",
                            compression=compression,
                            lines=lines,
                            chunksize=0,
                        )
                    )

                if lines:
                    # All chunksizes must return the same result
                    for chunksize in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100, 1000):
                        if infer & ((compression is None) | add_file_extension):
                            object_observed = []
                            for chunk in read_json(
                                filename=filepath,
                                mode="r",
                                compression="infer",
                                lines=True,
                                chunksize=chunksize,
                            ):
                                object_observed.extend(chunk)
                        else:
                            if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                                if "." in compression:
                                    mode = "r:" + compression.split(".")[1]
                                else:
                                    mode = "r:gz"

                                object_observed = []
                                for chunk in read_json(
                                    filename=filepath,
                                    mode=mode,
                                    compression="tar",
                                    lines=True,
                                    chunksize=chunksize,
                                ):
                                    object_observed.extend(chunk)
                            else:
                                object_observed = []
                                for chunk in read_json(
                                    filename=filepath,
                                    mode="r",
                                    compression=compression,
                                    lines=True,
                                    chunksize=chunksize,
                                ):
                                    object_observed.extend(chunk)

                        if isinstance(object_observed, list) & (
                            not isinstance(object_expected, list)
                        ):
                            object_observed = object_observed[0]

                        self.assertEqual(
                            object_expected,
                            object_observed,
                            "read_json() failed.\n"
                            "Parameters:\n"
                            f"  compression: {compression}\n"
                            f"  lines:       {lines}\n"
                            f"  chunksize:   {chunksize}\n"
                            f"Expected: '{object_expected}'\n"
                            f"Observed: '{object_observed}'",
                        )
                else:
                    # Specifying chunksize not None and lines=False
                    # should raise ValueError
                    self.assertRaises(
                        ValueError,
                        read_json,
                        filename=filepath,
                        mode="r",
                        compression=compression,
                        lines=False,
                        chunksize=1,
                    )

    def test_write_json(self):
        """test_write_json"""

        # Unknown compression raises NotImplementedError
        with TemporaryDirectory() as tmpdir:
            filename = Path(tmpdir) / "file"
            self.assertRaises(
                ValueError,
                write_json,
                filename=filename,
                object="{}",
                mode="w",
                compression="?",
            )

        object_expected_list = [
            {"A": "a", "B": 1, "C": 0.1, "D": True, "E": False, "F": None},
            [{"A": "a", "B": 1, "C": 0.1, "D": True, "E": False, "F": None}],
            0,
            0.1,
            True,
            False,
            None,
            "This is a test",
            ["This is a test"],
            ["This is a test", "This is another test"],
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            [0, 1, 3, "A", "b", "c", True, False, None],
        ]

        add_file_extension_list = [True, False]
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
        lines_list = [True, False]

        for (
            object_expected,
            add_file_extension,
            compression,
            infer,
            lines,
        ) in itertools.product(
            object_expected_list,
            add_file_extension_list,
            compression_list,
            infer_list,
            lines_list,
        ):
            with TemporaryDirectory() as tmpdir:
                filepath = Path(tmpdir) / "file"

                if add_file_extension:
                    if compression is None:
                        pass
                    elif compression == "gzip":
                        filepath = filepath.with_suffix(".gz")
                    elif compression == "bz2":
                        filepath = filepath.with_suffix(".bz2")
                    elif compression.startswith("tar") | (compression == "tgz"):
                        filepath = filepath.with_suffix("." + compression)
                    elif compression == "xz":
                        filepath = filepath.with_suffix(".xz")
                    elif compression == "zip":
                        filepath = filepath.with_suffix(".zip")
                    elif compression == "zstd":
                        filepath = filepath.with_suffix(".zst")

                if compression == "?":
                    self.assertRaises(
                        ValueError,
                        write_json,
                        filename=filepath,
                        object=object_expected,
                        mode="w",
                        compression="?",
                        level=None,
                        lines=lines,
                    )
                    continue

                # Write to new file
                mode = "w"
                if compression in ("tar.bz2", "tar.gz", "tar.xz"):
                    mode += ":" + compression.split(".")[1]
                elif compression == "tgz":
                    mode += ":gz"

                if infer & ((compression is None) | add_file_extension):
                    write_json(
                        filename=filepath,
                        object=object_expected,
                        mode=mode,
                        compression="infer",
                        level=None,
                        lines=lines,
                    )
                else:
                    if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                        write_json(
                            filename=filepath,
                            object=object_expected,
                            mode=mode,
                            compression="tar",
                            level=None,
                            lines=lines,
                        )
                    else:
                        write_json(
                            filename=filepath,
                            object=object_expected,
                            mode=mode,
                            compression=compression,
                            level=None,
                            lines=lines,
                        )

                # Read file contents
                if compression is None:
                    with open(filepath, mode="r") as handle:
                        content = handle.read()
                elif compression == "bz2":
                    with bz2.open(filepath, mode="r") as handle:
                        content = handle.read().decode()
                elif compression == "gzip":
                    with gzip.open(filepath, mode="r") as handle:
                        content = handle.read().decode()
                elif compression.startswith("tar") | (compression == "tgz"):
                    with tarfile.open(filepath, mode="r") as container_handle:
                        file_list = container_handle.getnames()
                        self.assertEqual(
                            len(file_list),
                            1,
                            "tar archive must contain exactly 1 file.",
                        )
                        file_in_archive = file_list[0]

                        with container_handle.extractfile(
                            file_in_archive
                        ) as file_handle:
                            content = file_handle.read().decode()
                elif compression == "xz":
                    with lzma.open(filepath, format=lzma.FORMAT_XZ, mode="r") as handle:
                        content = handle.read().decode()
                elif compression == "zip":
                    with zipfile.ZipFile(filepath, mode="r") as container_handle:
                        file_list = container_handle.namelist()
                        self.assertEqual(
                            len(file_list),
                            1,
                            "zip archive must contain exactly 1 file.",
                        )
                        file_in_archive = file_list[0]

                        with container_handle.open(
                            file_in_archive, mode="r"
                        ) as file_handle:
                            content = file_handle.read().decode()
                elif compression == "zstd":
                    with zstandard.open(filepath, mode="r") as handle:
                        content = handle.read()
                else:
                    raise NotImplementedError(compression)

                if lines:
                    object_observed = [
                        json.loads(line) for line in content.rstrip("\n").split("\n")
                    ]

                    if isinstance(object_observed, list) & (
                        not isinstance(object_expected, list)
                    ):
                        object_observed = object_observed[0]
                else:
                    object_observed = json.loads(content)

                self.assertEqual(
                    object_expected,
                    object_observed,
                    "write_json() failed.\n"
                    "Parameters:\n"
                    f"  mode: 'w'\n"
                    f"  compression: {compression}\n"
                    f"  lines:       {lines}\n"
                    f"Expected: '{object_expected}'\n"
                    f"Observed: '{object_observed}'",
                )

    def test_read_write_json(self):
        """test_read_write_json"""

        object_expected_list = [
            {"A": "a", "B": 1, "C": 0.1, "D": True, "E": False, "F": None},
            [{"A": "a", "B": 1, "C": 0.1, "D": True, "E": False, "F": None}],
            0,
            0.1,
            True,
            False,
            None,
            "This is a test",
            ["This is a test"],
            ["This is a test", "This is another test"],
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            [0, 1, 3, "A", "b", "c", True, False, None],
        ]

        add_file_extension_list = [True, False]
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
        lines_list = [True, False]

        for (
            object_expected,
            add_file_extension,
            compression,
            infer,
            lines,
        ) in itertools.product(
            object_expected_list,
            add_file_extension_list,
            compression_list,
            infer_list,
            lines_list,
        ):
            with TemporaryDirectory() as tmpdir:
                filepath = Path(tmpdir) / "file"

                if add_file_extension:
                    if compression is None:
                        pass
                    elif compression == "gzip":
                        filepath = filepath.with_suffix(".gz")
                    elif compression == "bz2":
                        filepath = filepath.with_suffix(".bz2")
                    elif compression.startswith("tar") | (compression == "tgz"):
                        filepath = filepath.with_suffix("." + compression)
                    elif compression == "xz":
                        filepath = filepath.with_suffix(".xz")
                    elif compression == "zip":
                        filepath = filepath.with_suffix(".zip")
                    elif compression == "zstd":
                        filepath = filepath.with_suffix(".zst")

                if compression == "?":
                    self.assertRaises(
                        ValueError,
                        write_json,
                        filename=filepath,
                        object=object_expected,
                        mode="w",
                        compression="?",
                        level=None,
                        lines=lines,
                    )
                    continue

                # Write to new file
                mode = "w"
                if compression in ("tar.bz2", "tar.gz", "tar.xz"):
                    mode += ":" + compression.split(".")[1]
                elif compression == "tgz":
                    mode += ":gz"

                if infer & ((compression is None) | add_file_extension):
                    write_json(
                        filename=filepath,
                        object=object_expected,
                        mode=mode,
                        compression="infer",
                        level=None,
                        lines=lines,
                    )
                else:
                    if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                        write_json(
                            filename=filepath,
                            object=object_expected,
                            mode=mode,
                            compression="tar",
                            level=None,
                            lines=lines,
                        )
                    else:
                        write_json(
                            filename=filepath,
                            object=object_expected,
                            mode=mode,
                            compression=compression,
                            level=None,
                            lines=lines,
                        )

                # Read file contents
                if infer & ((compression is None) | add_file_extension):
                    object_observed = read_json(
                        filename=filepath,
                        mode="r",
                        compression="infer",
                        lines=lines,
                        chunksize=None,
                    )
                else:
                    if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                        if "." in compression:
                            mode = "r:" + compression.split(".")[1]
                        else:
                            mode = "r:gz"

                        object_observed = read_json(
                            filename=filepath,
                            mode=mode,
                            compression="tar",
                            lines=lines,
                            chunksize=None,
                        )
                    else:
                        object_observed = read_json(
                            filename=filepath,
                            mode="r",
                            compression=compression,
                            lines=lines,
                            chunksize=None,
                        )

                if lines:
                    if isinstance(object_observed, list) & (
                        not isinstance(object_expected, list)
                    ):
                        object_observed = object_observed[0]

                self.assertEqual(
                    object_expected,
                    object_observed,
                    "read_json() failed.\n"
                    "Parameters:\n"
                    f"  compression: {compression}\n"
                    f"  lines:       {lines}\n"
                    f"Expected: '{object_expected}'\n"
                    f"Observed: '{object_observed}'",
                )

                # Read file contents with chunksize
                # chunksize = 0 should raise ValueError
                with self.assertRaises(ValueError):
                    next(
                        read_json(
                            filename=filepath,
                            mode="r",
                            compression=compression,
                            lines=lines,
                            chunksize=0,
                        )
                    )

                if lines:
                    # All chunksizes must return the same result
                    for chunksize in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100, 1000):
                        if infer & ((compression is None) | add_file_extension):
                            object_observed = []
                            for chunk in read_json(
                                filename=filepath,
                                mode="r",
                                compression="infer",
                                lines=True,
                                chunksize=chunksize,
                            ):
                                object_observed.extend(chunk)
                        else:
                            if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                                if "." in compression:
                                    mode = "r:" + compression.split(".")[1]
                                else:
                                    mode = "r:gz"

                                object_observed = []
                                for chunk in read_json(
                                    filename=filepath,
                                    mode=mode,
                                    compression="tar",
                                    lines=True,
                                    chunksize=chunksize,
                                ):
                                    object_observed.extend(chunk)
                            else:
                                object_observed = []
                                for chunk in read_json(
                                    filename=filepath,
                                    mode="r",
                                    compression=compression,
                                    lines=True,
                                    chunksize=chunksize,
                                ):
                                    object_observed.extend(chunk)

                        if isinstance(object_observed, list) & (
                            not isinstance(object_expected, list)
                        ):
                            object_observed = object_observed[0]

                        self.assertEqual(
                            object_expected,
                            object_observed,
                            "read_json() failed.\n"
                            "Parameters:\n"
                            f"  compression: {compression}\n"
                            f"  lines:       {lines}\n"
                            f"  chunksize:   {chunksize}\n"
                            f"Expected: '{object_expected}'\n"
                            f"Observed: '{object_observed}'",
                        )
                else:
                    # Specifying chunksize not None and lines=False
                    # should raise ValueError
                    self.assertRaises(
                        ValueError,
                        read_json,
                        filename=filepath,
                        mode="r",
                        compression=compression,
                        lines=False,
                        chunksize=1,
                    )

    def test_read_jsonl(self):
        """test_read_jsonl"""

        object_expected_list = [
            {"A": "a", "B": 1, "C": 0.1, "D": True, "E": False, "F": None},
            [{"A": "a", "B": 1, "C": 0.1, "D": True, "E": False, "F": None}],
            0,
            0.1,
            True,
            False,
            None,
            "This is a test",
            ["This is a test"],
            ["This is a test", "This is another test"],
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            [0, 1, 3, "A", "b", "c", True, False, None],
        ]

        add_file_extension_list = [True, False]
        infer_list = [True, False]
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
        )

        for (
            object_expected,
            add_file_extension,
            compression,
            infer,
        ) in itertools.product(
            object_expected_list, add_file_extension_list, compression_list, infer_list
        ):
            with TemporaryDirectory() as tmpdir:
                filepath = Path(tmpdir) / "file"

                if isinstance(object_expected, list):
                    object_expected_serialized = [
                        json.dumps(item) for item in object_expected
                    ]
                else:
                    object_expected_serialized = [json.dumps(object_expected)]

                content = "\n".join(object_expected_serialized) + "\n"
                content_bytes = content.encode()

                # Write to file
                if compression is None:
                    with open(filepath, mode="w") as handle:
                        handle.write(content)
                elif compression == "bz2":
                    if add_file_extension:
                        filepath = filepath.with_suffix(".bz2")
                    with bz2.open(filepath, mode="wb") as handle:
                        handle.write(content_bytes)
                elif compression == "gzip":
                    if add_file_extension:
                        filepath = filepath.with_suffix(".gz")
                    with gzip.open(filepath, mode="wb") as handle:
                        handle.write(content_bytes)
                elif compression == "xz":
                    if add_file_extension:
                        filepath = filepath.with_suffix(".xz")
                    with lzma.open(
                        filepath, format=lzma.FORMAT_XZ, mode="wb"
                    ) as handle:
                        handle.write(content_bytes)
                elif compression == "zip":
                    if add_file_extension:
                        filepath = filepath.with_suffix(".zip")
                    with zipfile.ZipFile(filepath, mode="w") as container_handle:
                        with container_handle.open("data", mode="w") as file_handle:
                            file_handle.write(content_bytes)
                elif compression in ("tar", "tar.bz2", "tar.gz", "tgz", "tar.xz"):
                    if add_file_extension:
                        filepath = filepath.with_suffix("." + compression)

                    tar_mode = "w"
                    if compression in ("tar.bz2", "tar.gz", "tar.xz"):
                        tar_mode += ":" + compression.split(".")[1]
                    elif compression == "tgz":
                        tar_mode += ":gz"

                    with tarfile.open(filepath, mode=tar_mode) as container_handle:
                        tar_info = tarfile.TarInfo(name="data")
                        tar_info.size = len(content_bytes)
                        container_handle.addfile(
                            tar_info,
                            fileobj=BytesIO(content_bytes),
                        )
                elif compression == "zstd":
                    if add_file_extension:
                        filepath = filepath.with_suffix(".zst")
                    with zstandard.open(filepath, mode="w") as handle:
                        handle.write(content)
                else:
                    raise NotImplementedError(compression)

                # Read file contents
                if infer & ((compression is None) | add_file_extension):
                    object_observed = read_jsonl(
                        filename=filepath,
                        mode="r",
                        compression="infer",
                        chunksize=None,
                    )
                else:
                    if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                        if "." in compression:
                            mode = "r:" + compression.split(".")[1]
                        else:
                            mode = "r:gz"

                        object_observed = read_jsonl(
                            filename=filepath,
                            mode=mode,
                            compression="tar",
                            chunksize=None,
                        )
                    else:
                        object_observed = read_jsonl(
                            filename=filepath,
                            mode="r",
                            compression=compression,
                            chunksize=None,
                        )

                if isinstance(object_observed, list) & (
                    not isinstance(object_expected, list)
                ):
                    object_observed = object_observed[0]

                self.assertEqual(
                    object_expected,
                    object_observed,
                    "read_jsonl() failed.\n"
                    "Parameters:\n"
                    f"  compression: {compression}\n"
                    f"Expected: '{object_expected}'\n"
                    f"Observed: '{object_observed}'",
                )

                # Read file contents with chunksize
                # chunksize = 0 should raise ValueError
                with self.assertRaises(ValueError):
                    next(
                        read_jsonl(
                            filename=filepath,
                            mode="r",
                            compression=compression,
                            chunksize=0,
                        )
                    )

                # All chunksizes must return the same result
                for chunksize in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100, 1000):
                    if infer & ((compression is None) | add_file_extension):
                        object_expected_list = []
                        for chunk in read_jsonl(
                            filename=filepath,
                            mode="r",
                            compression="infer",
                            chunksize=chunksize,
                        ):
                            object_expected_list.extend(chunk)
                    else:
                        if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                            if "." in compression:
                                mode = "r:" + compression.split(".")[1]
                            else:
                                mode = "r:gz"

                            object_expected_list = []
                            for chunk in read_jsonl(
                                filename=filepath,
                                mode=mode,
                                compression="tar",
                                chunksize=chunksize,
                            ):
                                object_expected_list.extend(chunk)
                        else:
                            object_expected_list = []
                            for chunk in read_jsonl(
                                filename=filepath,
                                mode="r",
                                compression=compression,
                                chunksize=chunksize,
                            ):
                                object_expected_list.extend(chunk)

                    if isinstance(object_observed, list) & (
                        not isinstance(object_expected, list)
                    ):
                        object_observed = object_observed[0]

                    self.assertEqual(
                        object_expected,
                        object_observed,
                        "read_jsonl() failed.\n"
                        "Parameters:\n"
                        f"  compression: {compression}\n"
                        f"Expected: '{object_expected}'\n"
                        f"Observed: '{object_observed}'",
                    )

    def test_write_jsonl(self):
        """test_write_jsonl"""

        # Unknown compression raises NotImplementedError
        with TemporaryDirectory() as tmpdir:
            filename = Path(tmpdir) / "file"
            self.assertRaises(
                ValueError,
                write_jsonl,
                filename=filename,
                object="{}",
                mode="w",
                compression="?",
            )

        object_expected_list = [
            {"A": "a", "B": 1, "C": 0.1, "D": True, "E": False, "F": None},
            [{"A": "a", "B": 1, "C": 0.1, "D": True, "E": False, "F": None}],
            0,
            0.1,
            True,
            False,
            None,
            "This is a test",
            ["This is a test"],
            ["This is a test", "This is another test"],
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            [0, 1, 3, "A", "b", "c", True, False, None],
        ]

        add_file_extension_list = [True, False]
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

        for (
            object_expected,
            add_file_extension,
            compression,
            infer,
        ) in itertools.product(
            object_expected_list, add_file_extension_list, compression_list, infer_list
        ):
            with TemporaryDirectory() as tmpdir:
                filepath = Path(tmpdir) / "file"

                if add_file_extension:
                    if compression is None:
                        pass
                    elif compression == "gzip":
                        filepath = filepath.with_suffix(".gz")
                    elif compression == "bz2":
                        filepath = filepath.with_suffix(".bz2")
                    elif compression.startswith("tar") | (compression == "tgz"):
                        filepath = filepath.with_suffix("." + compression)
                    elif compression == "xz":
                        filepath = filepath.with_suffix(".xz")
                    elif compression == "zip":
                        filepath = filepath.with_suffix(".zip")
                    elif compression == "zstd":
                        filepath = filepath.with_suffix(".zst")

                if compression == "?":
                    self.assertRaises(
                        ValueError,
                        write_jsonl,
                        filename=filepath,
                        object=object_expected,
                        mode="w",
                        compression="?",
                        level=None,
                    )
                    continue

                # Write to new file
                mode = "w"
                if compression in ("tar.bz2", "tar.gz", "tar.xz"):
                    mode += ":" + compression.split(".")[1]
                elif compression == "tgz":
                    mode += ":gz"

                if infer & ((compression is None) | add_file_extension):
                    write_jsonl(
                        filename=filepath,
                        object=object_expected,
                        mode=mode,
                        compression="infer",
                        level=None,
                    )
                else:
                    if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                        write_jsonl(
                            filename=filepath,
                            object=object_expected,
                            mode=mode,
                            compression="tar",
                            level=None,
                        )
                    else:
                        write_jsonl(
                            filename=filepath,
                            object=object_expected,
                            mode=mode,
                            compression=compression,
                            level=None,
                        )

                # Read file contents
                if compression is None:
                    with open(filepath, mode="r") as handle:
                        content = handle.read()
                elif compression == "bz2":
                    with bz2.open(filepath, mode="r") as handle:
                        content = handle.read().decode()
                elif compression == "gzip":
                    with gzip.open(filepath, mode="r") as handle:
                        content = handle.read().decode()
                elif compression.startswith("tar") | (compression == "tgz"):
                    with tarfile.open(filepath, mode="r") as container_handle:
                        file_list = container_handle.getnames()
                        self.assertEqual(
                            len(file_list),
                            1,
                            "tar archive must contain exactly 1 file.",
                        )
                        file_in_archive = file_list[0]

                        with container_handle.extractfile(
                            file_in_archive
                        ) as file_handle:
                            content = file_handle.read().decode()
                elif compression == "xz":
                    with lzma.open(filepath, format=lzma.FORMAT_XZ, mode="r") as handle:
                        content = handle.read().decode()
                elif compression == "zip":
                    with zipfile.ZipFile(filepath, mode="r") as container_handle:
                        file_list = container_handle.namelist()
                        self.assertEqual(
                            len(file_list),
                            1,
                            "zip archive must contain exactly 1 file.",
                        )
                        file_in_archive = file_list[0]

                        with container_handle.open(
                            file_in_archive, mode="r"
                        ) as file_handle:
                            content = file_handle.read().decode()
                elif compression == "zstd":
                    with zstandard.open(filepath, mode="r") as handle:
                        content = handle.read()
                else:
                    raise NotImplementedError(compression)

                object_observed = [
                    json.loads(line) for line in content.rstrip("\n").split("\n")
                ]

                if isinstance(object_observed, list) & (
                    not isinstance(object_expected, list)
                ):
                    object_observed = object_observed[0]

                self.assertEqual(
                    object_expected,
                    object_observed,
                    "write_jsonl() failed.\n"
                    "Parameters:\n"
                    f"  mode: '{mode}'\n"
                    f"  compression: {compression}\n"
                    f"Expected: '{object_expected}'\n"
                    f"Observed: '{object_observed}'",
                )

    def test_read_write_jsonl(self):
        """test_read_write_jsonl"""

        object_expected_list = [
            {"A": "a", "B": 1, "C": 0.1, "D": True, "E": False, "F": None},
            [{"A": "a", "B": 1, "C": 0.1, "D": True, "E": False, "F": None}],
            0,
            0.1,
            True,
            False,
            None,
            "This is a test",
            ["This is a test"],
            ["This is a test", "This is another test"],
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            [0, 1, 3, "A", "b", "c", True, False, None],
        ]

        add_file_extension_list = [True, False]
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

        for (
            object_expected,
            add_file_extension,
            compression,
            infer,
        ) in itertools.product(
            object_expected_list, add_file_extension_list, compression_list, infer_list
        ):
            with TemporaryDirectory() as tmpdir:
                filepath = Path(tmpdir) / "file"

                if add_file_extension:
                    if compression is None:
                        pass
                    elif compression == "gzip":
                        filepath = filepath.with_suffix(".gz")
                    elif compression == "bz2":
                        filepath = filepath.with_suffix(".bz2")
                    elif compression.startswith("tar") | (compression == "tgz"):
                        filepath = filepath.with_suffix("." + compression)
                    elif compression == "xz":
                        filepath = filepath.with_suffix(".xz")
                    elif compression == "zip":
                        filepath = filepath.with_suffix(".zip")
                    elif compression == "zstd":
                        filepath = filepath.with_suffix(".zst")

                if compression == "?":
                    self.assertRaises(
                        ValueError,
                        write_jsonl,
                        filename=filepath,
                        object=object_expected,
                        mode="w",
                        compression="?",
                        level=None,
                    )
                    continue

                # Write to new file
                mode = "w"
                if compression in ("tar.bz2", "tar.gz", "tar.xz"):
                    mode += ":" + compression.split(".")[1]
                elif compression == "tgz":
                    mode += ":gz"

                if infer & ((compression is None) | add_file_extension):
                    write_jsonl(
                        filename=filepath,
                        object=object_expected,
                        mode=mode,
                        compression="infer",
                        level=None,
                    )
                else:
                    if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                        write_jsonl(
                            filename=filepath,
                            object=object_expected,
                            mode=mode,
                            compression="tar",
                            level=None,
                        )
                    else:
                        write_jsonl(
                            filename=filepath,
                            object=object_expected,
                            mode=mode,
                            compression=compression,
                            level=None,
                        )

                # Read file contents
                if infer & ((compression is None) | add_file_extension):
                    object_observed = read_jsonl(
                        filename=filepath,
                        mode="r",
                        compression="infer",
                        chunksize=None,
                    )
                else:
                    if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                        if "." in compression:
                            mode = "r:" + compression.split(".")[1]
                        else:
                            mode = "r:gz"

                        object_observed = read_jsonl(
                            filename=filepath,
                            mode=mode,
                            compression="tar",
                            chunksize=None,
                        )
                    else:
                        object_observed = read_jsonl(
                            filename=filepath,
                            mode="r",
                            compression=compression,
                            chunksize=None,
                        )

                if isinstance(object_observed, list) & (
                    not isinstance(object_expected, list)
                ):
                    object_observed = object_observed[0]

                self.assertEqual(
                    object_expected,
                    object_observed,
                    "read_jsonl() failed.\n"
                    "Parameters:\n"
                    f"  compression: {compression}\n"
                    f"Expected: '{object_expected}'\n"
                    f"Observed: '{object_observed}'",
                )

                # Read file contents with chunksize
                # chunksize = 0 should raise ValueError
                with self.assertRaises(ValueError):
                    next(
                        read_jsonl(
                            filename=filepath,
                            mode="r",
                            compression=compression,
                            chunksize=0,
                        )
                    )

                # All chunksizes must return the same result
                for chunksize in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100, 1000):
                    if infer & ((compression is None) | add_file_extension):
                        object_expected_list = []
                        for chunk in read_jsonl(
                            filename=filepath,
                            mode="r",
                            compression="infer",
                            chunksize=chunksize,
                        ):
                            object_expected_list.extend(chunk)
                    else:
                        if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                            if "." in compression:
                                mode = "r:" + compression.split(".")[1]
                            else:
                                mode = "r:gz"

                            object_expected_list = []
                            for chunk in read_jsonl(
                                filename=filepath,
                                mode=mode,
                                compression="tar",
                                chunksize=chunksize,
                            ):
                                object_expected_list.extend(chunk)
                        else:
                            object_expected_list = []
                            for chunk in read_jsonl(
                                filename=filepath,
                                mode="r",
                                compression=compression,
                                chunksize=chunksize,
                            ):
                                object_expected_list.extend(chunk)

                    if isinstance(object_observed, list) & (
                        not isinstance(object_expected, list)
                    ):
                        object_observed = object_observed[0]

                    self.assertEqual(
                        object_expected,
                        object_observed,
                        "read_jsonl() failed.\n"
                        "Parameters:\n"
                        f"  compression: {compression}\n"
                        f"Expected: '{object_expected}'\n"
                        f"Observed: '{object_observed}'",
                    )
